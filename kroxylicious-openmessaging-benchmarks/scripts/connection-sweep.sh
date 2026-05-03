#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

DEFAULT_WORKLOAD="1topic-1kb"
DEFAULT_SCENARIO="encryption"
DEFAULT_STEPS="1,2,4,8,16"
DEFAULT_PROFILE_VALUES="${SCRIPT_DIR}/../helm/kroxylicious-benchmark/scenarios/connection-sweep-values.yaml"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [options] --per-producer-rate <n> --output-dir <dir>

Sweeps the number of producer connections to a single proxy pod while holding
per-producer rate fixed. Infrastructure is deployed once; run-benchmark.sh is
invoked at each step (reusing the deployment). Consumers are scaled to match
producers at each step.

Use this to find the aggregate throughput ceiling of a single proxy pod —
the point where adding more connections no longer increases total throughput.

Options:
  --output-dir <dir>        Directory to write results into (required)
  --scenario <name>         Scenario to run (default: ${DEFAULT_SCENARIO})
  --per-producer-rate <n>   Fixed producer rate per producer, msg/sec (required)
  --steps <list>            Comma-separated producer counts to probe
                            (default: ${DEFAULT_STEPS})
  --workload <name>         OMB workload to use (default: ${DEFAULT_WORKLOAD})
  --profile <values-file>   Additional Helm values layered on top of the scenario
                            (default: scenarios/connection-sweep-values.yaml)
                            May be specified multiple times; files are applied in order.
  --cluster-overrides <file> Helm values file with cluster-specific settings (storage class,
                            images, resource limits). Applied after --profile files so cluster
                            settings always win.
  --set <key=value>         Pass a Helm --set override to run-benchmark.sh (may be repeated)
  --skip-proxy-isolation    Pass --skip-proxy-isolation to run-benchmark.sh (see that script for details)
  --dry-run                 Print step sequence and planned probes without running anything
  -h, --help                Show this help

Environment:
  NAMESPACE              Kubernetes namespace (default: kafka)
  KAFKA_READY_TIMEOUT    Timeout waiting for Kafka cluster (default: 600s)
  POD_READY_TIMEOUT      Timeout waiting for pods (default: 300s)
  PROBE_TIMEOUT          Max seconds to wait for a single probe to complete (default: 3600)

Examples:
  # Sweep 1→16 producers at 30k msg/sec each, encryption scenario
  $(basename "$0") --per-producer-rate 30000 --output-dir ./results/conn-sweep/

  # Preview step sequence without deploying anything
  $(basename "$0") --dry-run --per-producer-rate 30000 --steps 1,2,4,8,16

  # Sweep a different scenario with cluster-specific overrides
  $(basename "$0") --scenario proxy-no-filters \\
    --per-producer-rate 50000 \\
    --cluster-overrides ~/cluster-overrides.yaml \\
    --output-dir ./results/conn-sweep-no-filters/
EOF
    exit 1
}

# --- Argument parsing ---

OUTPUT_DIR=""
SCENARIO="${DEFAULT_SCENARIO}"
PER_PRODUCER_RATE=""
STEPS="${DEFAULT_STEPS}"
WORKLOAD="${DEFAULT_WORKLOAD}"
PROFILE_VALUES=("${DEFAULT_PROFILE_VALUES}")
CLUSTER_OVERRIDES=""
HELM_SET_ARGS=()
SKIP_PROXY_ISOLATION=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir)           OUTPUT_DIR="$2";           shift 2 ;;
        --scenario)             SCENARIO="$2";             shift 2 ;;
        --per-producer-rate)    PER_PRODUCER_RATE="$2";    shift 2 ;;
        --steps)                STEPS="$2";                shift 2 ;;
        --workload)             WORKLOAD="$2";             shift 2 ;;
        --profile)              PROFILE_VALUES+=("$2");    shift 2 ;;
        --cluster-overrides)    CLUSTER_OVERRIDES="$2";    shift 2 ;;
        --set)                  HELM_SET_ARGS+=("$2");     shift 2 ;;
        --skip-proxy-isolation) SKIP_PROXY_ISOLATION=true; shift   ;;
        --dry-run)              DRY_RUN=true;              shift   ;;
        -h|--help)              usage ;;
        -*)                     echo "Error: unknown option '$1'" >&2; usage ;;
        *)                      echo "Error: unexpected argument '$1'" >&2; usage ;;
    esac
done

if [[ -z "${OUTPUT_DIR}" && "${DRY_RUN}" == "false" ]]; then
    echo "Error: --output-dir is required" >&2
    usage
fi

if [[ -z "${PER_PRODUCER_RATE}" ]]; then
    echo "Error: --per-producer-rate is required" >&2
    usage
fi

if ! [[ "${PER_PRODUCER_RATE}" =~ ^[0-9]+$ ]]; then
    echo "Error: --per-producer-rate must be a positive integer, got: ${PER_PRODUCER_RATE}" >&2
    exit 1
fi

IFS=',' read -ra STEP_ARRAY <<< "${STEPS}"
for step in "${STEP_ARRAY[@]}"; do
    if ! [[ "${step}" =~ ^[0-9]+$ ]] || [[ "${step}" -lt 1 ]]; then
        echo "Error: each step must be a positive integer, got: ${step}" >&2
        exit 1
    fi
done

for profile_file in ${PROFILE_VALUES[@]+"${PROFILE_VALUES[@]}"}; do
    if [[ ! -f "${profile_file}" ]]; then
        echo "Error: --profile file not found: ${profile_file}" >&2
        exit 1
    fi
done

if [[ -n "${CLUSTER_OVERRIDES}" && ! -f "${CLUSTER_OVERRIDES}" ]]; then
    echo "Error: --cluster-overrides file not found: ${CLUSTER_OVERRIDES}" >&2
    exit 1
fi

# --- Dry-run ---

if [[ "${DRY_RUN}" == "true" ]]; then
    echo "=== Dry run: $(basename "$0") ==="
    echo "Scenario:         ${SCENARIO}"
    echo "Workload:         ${WORKLOAD}"
    echo "Per-producer rate: $(printf "%'d" "${PER_PRODUCER_RATE}") msg/sec"
    echo "Output dir:       ${OUTPUT_DIR:-<not set>}"
    [[ ${#PROFILE_VALUES[@]} -gt 0 ]] && echo "Profiles:         ${PROFILE_VALUES[*]}"
    [[ -n "${CLUSTER_OVERRIDES}" ]] && echo "Cluster:          ${CLUSTER_OVERRIDES}"
    echo ""
    echo "Step sequence (producers → aggregate rate):"
    for step in "${STEP_ARRAY[@]}"; do
        aggregate=$(( step * PER_PRODUCER_RATE ))
        printf "  %2d producer(s) → %s msg/sec aggregate\n" \
            "${step}" "$(printf "%'d" "${aggregate}")"
    done
    echo ""
    echo "Total probes: ${#STEP_ARRAY[@]}"
    exit 0
fi

# --- Summary table ---

print_summary() {
    if ! command -v jq &>/dev/null; then
        echo "Warning: jq not found — skipping summary table" >&2
        return
    fi

    local scenario_dir="${OUTPUT_DIR}/${SCENARIO}"
    local steps=()
    while IFS= read -r d; do
        steps+=("${d}")
    done < <(find "${scenario_dir}" -maxdepth 1 -name 'producers-*' -type d 2>/dev/null \
        | xargs -r -n1 basename | sed 's/^producers-//' | sort -n)

    if [[ ${#steps[@]} -eq 0 ]]; then
        echo "No results found in ${scenario_dir} — cannot produce summary" >&2
        return
    fi

    local any_saturated=false
    echo "=== Summary: ${SCENARIO} ==="
    echo "(achieved = mean publish rate; p99 = mean end-to-end latency p99)"
    echo ""

    printf "%-12s  %-14s  %-20s  %-14s\n" \
        "Producers" "Target (msg/s)" "Achieved (msg/s)" "p99 latency"
    printf '%s\n' "$(printf '%*s' 70 '' | tr ' ' '-')"

    for n in "${steps[@]}"; do
        local target=$(( n * PER_PRODUCER_RATE ))
        local rf="${scenario_dir}/producers-${n}/result.json"
        local rd="${scenario_dir}/producers-${n}"

        if [[ ! -f "${rf}" ]]; then
            if [[ -d "${rd}" ]]; then
                printf "%-12s  %-14s  %-20s  %-14s\n" \
                    "${n}" "$(printf "%'d" "${target}")" "✗ errored" "—"
            else
                printf "%-12s  %-14s  %-20s  %-14s\n" \
                    "${n}" "$(printf "%'d" "${target}")" "(not run)" "—"
            fi
            continue
        fi

        local achieved sat p99
        achieved=$(jq '[.publishRate[]] | add / length' "${rf}")
        sat=$(awk "BEGIN { print (${achieved} < ${target} * 0.95) ? 1 : 0 }")

        if [[ "${sat}" == "1" ]]; then
            local achieved_int
            achieved_int=$(printf '%.0f' "${achieved}")
            any_saturated=true
            printf "%-12s  %-14s  %-20s  %-14s\n" \
                "${n}" "$(printf "%'d" "${target}")" "$(printf "%'d" "${achieved_int}") [1]" "—"
        else
            p99=$(jq '.aggregatedEndToEndLatency99pct' "${rf}")
            printf "%-12s  %-14s  %-20s  %-14s\n" \
                "${n}" "$(printf "%'d" "${target}")" \
                "$(printf '%.0f' "${achieved}")" \
                "$(printf '%.2f ms' "${p99}")"
        fi
    done

    echo ""
    if [[ "${any_saturated}" == "true" ]]; then
        echo "[1] Saturated: achieved rate fell more than 5% below target — p99 latency is not meaningful."
    fi
}

# --- Main ---

mkdir -p "${OUTPUT_DIR}"
SCENARIO_OUTPUT="${OUTPUT_DIR}/${SCENARIO}"

cat > "${OUTPUT_DIR}/run-metadata.json" <<METADATA_EOF
{
  "gitCommit": "$(git rev-parse HEAD 2>/dev/null || echo "unknown")",
  "gitBranch": "$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "scenario": "${SCENARIO}",
  "workload": "${WORKLOAD}",
  "perProducerRate": ${PER_PRODUCER_RATE},
  "steps": "${STEPS}",
  "profiles": "$(IFS=,; echo "${PROFILE_VALUES[*]+"${PROFILE_VALUES[*]}"}")",
  "clusterOverrides": "${CLUSTER_OVERRIDES:-}"
}
METADATA_EOF

echo "=== Connection sweep ==="
echo "Scenario:          ${SCENARIO}"
echo "Workload:          ${WORKLOAD}"
echo "Per-producer rate: $(printf "%'d" "${PER_PRODUCER_RATE}") msg/sec"
echo "Steps:             ${STEPS} producers"
echo "Output dir:        ${OUTPUT_DIR}"
[[ ${#PROFILE_VALUES[@]} -gt 0 ]] && echo "Profiles:          ${PROFILE_VALUES[*]}"
[[ -n "${CLUSTER_OVERRIDES}" ]] && echo "Cluster:           ${CLUSTER_OVERRIDES}"
echo ""

TOTAL_PROBES=${#STEP_ARRAY[@]}

FAILED=false
for i in "${!STEP_ARRAY[@]}"; do
    N="${STEP_ARRAY[$i]}"
    PROBE_NUM=$((i + 1))
    AGGREGATE=$(( N * PER_PRODUCER_RATE ))
    PROBE_OUTPUT="${SCENARIO_OUTPUT}/producers-${N}"

    echo "--- Probe ${PROBE_NUM}/${TOTAL_PROBES}: ${N} producer(s) → $(printf '%'"'"'d' "${AGGREGATE}") msg/sec aggregate ---"

    RB_ARGS=(
        --producer-rate "${AGGREGATE}"
        --producers-per-topic "${N}"
        --consumers-per-subscription "${N}"
    )
    [[ $i -gt 0 ]]                     && RB_ARGS+=(--skip-deploy)
    [[ $i -lt $((TOTAL_PROBES - 1)) ]] && RB_ARGS+=(--skip-teardown)
    for profile_file in ${PROFILE_VALUES[@]+"${PROFILE_VALUES[@]}"}; do
        RB_ARGS+=(--profile "${profile_file}")
    done
    [[ -n "${CLUSTER_OVERRIDES}" ]] && RB_ARGS+=(--cluster-overrides "${CLUSTER_OVERRIDES}")
    for set_arg in "${HELM_SET_ARGS[@]+"${HELM_SET_ARGS[@]}"}"; do
        RB_ARGS+=(--set "${set_arg}")
    done
    [[ "${SKIP_PROXY_ISOLATION}" == "true" ]] && RB_ARGS+=(--skip-proxy-isolation)

    if ! "${SCRIPT_DIR}/run-benchmark.sh" ${RB_ARGS[@]+"${RB_ARGS[@]}"} \
            "${SCENARIO}" "${WORKLOAD}" "${PROBE_OUTPUT}"; then
        FAILED=true
        break
    fi
done

[[ "${FAILED}" == "true" ]] && echo "  Sweep stopped early for ${SCENARIO}" >&2
echo ""

echo "=== Sweep complete ==="
echo "Results written to: ${OUTPUT_DIR}"
echo ""
print_summary
