#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

DEFAULT_WORKLOAD="1topic-1kb"
DEFAULT_SCENARIOS="baseline,proxy-no-filters"
DEFAULT_STEP_PERCENT=10

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [options] --output-dir <dir>

Runs a rate sweep across one or more scenarios to find the saturation point
and measure proxy overhead. For each scenario, infrastructure is deployed once,
run-benchmark.sh is invoked at each rate in the sweep (reusing the deployment),
then infrastructure is torn down. Kafka topics are reset between probes.

Results are written as JSON per rate step. A summary table is printed at the end.
If baseline results are available (from this run or via --baseline-from), a
side-by-side comparison is produced.

Options:
  --output-dir <dir>        Directory to write results into (required)
  --scenarios <list>        Comma-separated scenarios to run
                            (default: ${DEFAULT_SCENARIOS})
  --baseline-from <dir>     Read pre-existing baseline results from here;
                            implies baseline is excluded from --scenarios
  --min-rate <n>            Minimum producer rate in msg/sec (required)
  --max-rate <n>            Maximum producer rate in msg/sec (required)
  --step-percent <n>        Step size as a percentage of the range (default: ${DEFAULT_STEP_PERCENT})
  --workload <name>         OMB workload to use (default: ${DEFAULT_WORKLOAD})
  --profile <values-file>   Additional Helm values layered on top of each scenario
  --set <key=value>         Pass a Helm --set override to run-benchmark.sh (may be repeated)
  --dry-run                 Print rate sequence and planned steps without running anything
  -h, --help                Show this help

Environment:
  NAMESPACE              Kubernetes namespace (default: kafka)
  KAFKA_READY_TIMEOUT    Timeout waiting for Kafka cluster (default: 600s)
  POD_READY_TIMEOUT      Timeout waiting for pods (default: 300s)
  PROBE_TIMEOUT          Max seconds to wait for a single probe to complete (default: 3600)

Examples:
  # Full sweep — baseline then proxy, 10% steps from 10k to 110k msg/sec
  $(basename "$0") --min-rate 10000 --max-rate 110000 --output-dir ./results/run-1/

  # Preview rate sequence without deploying anything
  $(basename "$0") --dry-run --min-rate 10000 --max-rate 110000 --step-percent 25

  # Reuse existing baseline, sweep a new proxy configuration
  $(basename "$0") --scenarios proxy-no-filters \\
    --baseline-from ./results/run-1/baseline/ \\
    --output-dir ./results/proxy-config-2/
EOF
    exit 1
}

# --- Argument parsing ---

OUTPUT_DIR=""
SCENARIOS="${DEFAULT_SCENARIOS}"
BASELINE_FROM=""
MIN_RATE=""
MAX_RATE=""
STEP_PERCENT="${DEFAULT_STEP_PERCENT}"
WORKLOAD="${DEFAULT_WORKLOAD}"
PROFILE_VALUES=""
HELM_SET_ARGS=()
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir)    OUTPUT_DIR="$2";           shift 2 ;;
        --scenarios)     SCENARIOS="$2";            shift 2 ;;
        --baseline-from) BASELINE_FROM="$2";        shift 2 ;;
        --min-rate)      MIN_RATE="$2";             shift 2 ;;
        --max-rate)      MAX_RATE="$2";             shift 2 ;;
        --step-percent)  STEP_PERCENT="$2";         shift 2 ;;
        --workload)      WORKLOAD="$2";             shift 2 ;;
        --profile)       PROFILE_VALUES="$2";       shift 2 ;;
        --set)           HELM_SET_ARGS+=("$2");     shift 2 ;;
        --dry-run)       DRY_RUN=true;              shift   ;;
        -h|--help)       usage ;;
        -*)              echo "Error: unknown option '$1'" >&2; usage ;;
        *)               echo "Error: unexpected argument '$1'" >&2; usage ;;
    esac
done

if [[ -z "${OUTPUT_DIR}" && "${DRY_RUN}" == "false" ]]; then
    echo "Error: --output-dir is required" >&2
    usage
fi

if [[ -z "${MIN_RATE}" ]]; then
    echo "Error: --min-rate is required" >&2
    usage
fi

if [[ -z "${MAX_RATE}" ]]; then
    echo "Error: --max-rate is required" >&2
    usage
fi

if [[ "${MAX_RATE}" -lt "${MIN_RATE}" ]]; then
    echo "Error: --max-rate (${MAX_RATE}) must be >= --min-rate (${MIN_RATE})" >&2
    exit 1
fi

if [[ -n "${BASELINE_FROM}" && ! -d "${BASELINE_FROM}" ]]; then
    echo "Error: --baseline-from directory not found: ${BASELINE_FROM}" >&2
    exit 1
fi

if [[ -n "${PROFILE_VALUES}" && ! -f "${PROFILE_VALUES}" ]]; then
    echo "Error: --profile file not found: ${PROFILE_VALUES}" >&2
    exit 1
fi

# --- Rate progression ---

# Computes a linear rate sequence from MIN_RATE to MAX_RATE.
# The fixed increment is (MAX_RATE - MIN_RATE) * STEP_PERCENT / 100, so
# step=10% always produces 10 evenly-spaced probes regardless of the range.
# MAX_RATE is always included as the final probe.
rate_sequence() {
    awk -v min="${MIN_RATE}" -v max="${MAX_RATE}" -v step="${STEP_PERCENT}" 'BEGIN {
        increment = (max - min) * step / 100
        if (min == max) { printf "%d\n", min; exit }
        if (increment <= 0) {
            print "Error: step percent too small or range too narrow" > "/dev/stderr"
            exit 1
        }
        rate = min
        while (rate < max) {
            printf "%d\n", rate
            rate += increment
        }
        printf "%d\n", max
    }'
}

# --- Dry-run ---

if [[ "${DRY_RUN}" == "true" ]]; then
    echo "=== Dry run: $(basename "$0") ==="
    echo "Scenarios:    ${SCENARIOS}"
    echo "Workload:     ${WORKLOAD}"
    echo "Output dir:   ${OUTPUT_DIR:-<not set>}"
    [[ -n "${BASELINE_FROM}" ]] && echo "Baseline from: ${BASELINE_FROM}"
    [[ -n "${PROFILE_VALUES}" ]] && echo "Profile:       ${PROFILE_VALUES}"
    echo ""
    STEP_INCREMENT=$(awk "BEGIN { printf \"%'d\", (${MAX_RATE} - ${MIN_RATE}) * ${STEP_PERCENT} / 100 }")
    echo "Rate sequence (min=$(printf "%'d" "${MIN_RATE}"), max=$(printf "%'d" "${MAX_RATE}"), step=${STEP_PERCENT}% (+${STEP_INCREMENT} msg/sec fixed increment)):"
    rate_sequence | awk '{ printf "  %'"'"'d msg/sec\n", $1 }'
    echo ""
    RATE_COUNT=$(rate_sequence | wc -l | tr -d ' ')
    echo "Total probes per scenario: ${RATE_COUNT}"
    exit 0
fi

# --- Summary table ---

print_summary() {
    if ! command -v jq &>/dev/null; then
        echo "Warning: jq not found — skipping summary table" >&2
        return
    fi

    local baseline_dir=""
    if [[ -n "${BASELINE_FROM}" ]]; then
        baseline_dir="${BASELINE_FROM}"
    elif [[ -d "${OUTPUT_DIR}/baseline" ]]; then
        baseline_dir="${OUTPUT_DIR}/baseline"
    fi

    local ref_dir="${baseline_dir:-${OUTPUT_DIR}/${SCENARIO_ARRAY[0]}}"
    local rates=()
    while IFS= read -r rate; do
        rates+=("${rate}")
    done < <(find "${ref_dir}" -maxdepth 1 -name 'rate-*' -type d 2>/dev/null \
        | sed 's|.*rate-||' | sort -n)

    if [[ ${#rates[@]} -eq 0 ]]; then
        echo "No results found in ${ref_dir} — cannot produce summary" >&2
        return
    fi

    local other_scenarios=()
    for s in "${SCENARIO_ARRAY[@]}"; do
        [[ "${s}" != "baseline" ]] && other_scenarios+=("${s}")
    done

    echo "=== Summary ==="
    echo "(achieved = mean publish rate; p99 = mean end-to-end latency p99; sat@N(-D) = saturated at N msg/s, D short of target)"
    echo ""

    printf "%-14s" "Target (msg/s)"
    if [[ -n "${baseline_dir}" ]]; then
        printf "  %-20s  %-14s" "Baseline achieved" "Baseline p99"
    fi
    for s in "${other_scenarios[@]+"${other_scenarios[@]}"}"; do
        printf "  %-20s  %-14s" "${s} achieved" "${s} p99"
        [[ -n "${baseline_dir}" ]] && printf "  %-20s" "Overhead (abs/rel)"
    done
    printf "\n"
    printf '%s\n' "$(printf '%*s' 100 '' | tr ' ' '-')"

    for rate in "${rates[@]}"; do
        printf "%-14s" "$(printf '%'"'"'d' "${rate}")"

        local baseline_p99=""
        if [[ -n "${baseline_dir}" ]]; then
            local bf="${baseline_dir}/rate-${rate}/result.json"
            local bd="${baseline_dir}/rate-${rate}"
            if [[ -f "${bf}" ]]; then
                local b_achieved b_sat b_p99
                b_achieved=$(jq '[.publishRate[]] | add / length' "${bf}")
                b_sat=$(awk "BEGIN { print (${b_achieved} < ${rate} * 0.95) ? 1 : 0 }")
                if [[ "${b_sat}" == "1" ]]; then
                    local b_delta
                    b_delta=$(awk "BEGIN { printf "%\047d", ${rate} - ${b_achieved} }")
                    printf "  %-20s  %-14s" "sat@$(printf '%\'\''d' "$(printf '%.0f' "${b_achieved}")")(-${b_delta})" "—"
                else
                    b_p99=$(jq '[.endToEndLatency99pct[]] | add / length' "${bf}")
                    baseline_p99="${b_p99}"
                    printf "  %-20s  %-14s" \
                        "$(printf '%.0f' "${b_achieved}")" \
                        "$(printf '%.2f ms' "${b_p99}")"
                fi
            elif [[ -d "${bd}" ]]; then
                printf "  %-20s  %-14s" "✗ errored" "—"
            else
                printf "  %-20s  %-14s" "(not run)" "—"
            fi
        fi

        for s in "${other_scenarios[@]+"${other_scenarios[@]}"}"; do
            local sf="${OUTPUT_DIR}/${s}/rate-${rate}/result.json"
            local sd="${OUTPUT_DIR}/${s}/rate-${rate}"
            if [[ ! -f "${sf}" ]]; then
                if [[ -d "${sd}" ]]; then
                    printf "  %-18s  %-12s" "✗ errored" "—"
                else
                    printf "  %-18s  %-12s" "(not run)" "—"
                fi
                [[ -n "${baseline_dir}" ]] && printf "  %-12s" "—"
                continue
            fi
            local s_achieved s_sat s_p99
            s_achieved=$(jq '[.publishRate[]] | add / length' "${sf}")
            s_sat=$(awk "BEGIN { print (${s_achieved} < ${rate} * 0.95) ? 1 : 0 }")
            if [[ "${s_sat}" == "1" ]]; then
                local s_delta
                s_delta=$(awk "BEGIN { printf "%\047d", ${rate} - ${s_achieved} }")
                printf "  %-20s  %-14s" "sat@$(printf '%\'\''d' "$(printf '%.0f' "${s_achieved}")")(-${s_delta})" "—"
                [[ -n "${baseline_dir}" ]] && printf "  %-20s" "—"
            else
                s_p99=$(jq '[.endToEndLatency99pct[]] | add / length' "${sf}")
                printf "  %-20s  %-14s" \
                    "$(printf '%.0f' "${s_achieved}")" \
                    "$(printf '%.2f ms' "${s_p99}")"
                if [[ -n "${baseline_dir}" ]]; then
                    if [[ -n "${baseline_p99}" ]]; then
                        local overhead
                        overhead=$(awk -v s="${s_p99}" -v b="${baseline_p99}" 'BEGIN { abs = s - b; rel = (b > 0) ? (abs / b * 100) : 0; printf "+%.2f ms (+%.0f%%)", abs, rel }')
                        printf "  %-20s" "${overhead}"
                    else
                        printf "  %-20s" "—"
                    fi
                fi
            fi
        done
        printf "\n"
    done
    echo ""
}

# --- Main ---

# If --baseline-from is provided, exclude baseline from the run list.
SCENARIO_LIST="${SCENARIOS}"
if [[ -n "${BASELINE_FROM}" ]]; then
    SCENARIO_LIST=$(echo "${SCENARIO_LIST}" | tr ',' '\n' | grep -v '^baseline$' | tr '\n' ',' | sed 's/,$//')
    if [[ -z "${SCENARIO_LIST}" ]]; then
        echo "Error: --baseline-from excludes baseline but no other scenarios remain in --scenarios" >&2
        exit 1
    fi
fi

mkdir -p "${OUTPUT_DIR}"
IFS=',' read -ra SCENARIO_ARRAY <<< "${SCENARIO_LIST}"

cat > "${OUTPUT_DIR}/run-metadata.json" <<METADATA_EOF
{
  "gitCommit": "$(git rev-parse HEAD 2>/dev/null || echo "unknown")",
  "gitBranch": "$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "minRate": ${MIN_RATE},
  "maxRate": ${MAX_RATE},
  "stepPercent": ${STEP_PERCENT},
  "scenarios": "${SCENARIO_LIST}",
  "workload": "${WORKLOAD}",
  "profile": "${PROFILE_VALUES:-}"
}
METADATA_EOF

echo "=== Rate sweep ==="
echo "Scenarios:  ${SCENARIO_LIST}"
echo "Workload:   ${WORKLOAD}"
echo "Output dir: ${OUTPUT_DIR}"
echo "Rates:      ${MIN_RATE}–${MAX_RATE} msg/sec (${STEP_PERCENT}% steps)"
[[ -n "${BASELINE_FROM}" ]] && echo "Baseline:   ${BASELINE_FROM}"
echo ""

for SCENARIO in "${SCENARIO_ARRAY[@]}"; do
    SCENARIO_OUTPUT="${OUTPUT_DIR}/${SCENARIO}"

    echo "=== Scenario: ${SCENARIO} ==="

    RATES=()
    while IFS= read -r r; do RATES+=("$r"); done < <(rate_sequence)
    TOTAL_PROBES=${#RATES[@]}

    FAILED=false
    for i in "${!RATES[@]}"; do
        RATE="${RATES[$i]}"
        PROBE_NUM=$((i + 1))
        PROBE_OUTPUT="${SCENARIO_OUTPUT}/rate-${RATE}"

        echo "--- Probe ${PROBE_NUM}/${TOTAL_PROBES}: $(printf '%'"'"'d' "${RATE}") msg/sec ---"

        # Build run-benchmark.sh args.
        # First probe deploys (no --skip-deploy); last probe tears down (no --skip-teardown).
        RB_ARGS=(--producer-rate "${RATE}")
        [[ $i -gt 0 ]]                     && RB_ARGS+=(--skip-deploy)
        [[ $i -lt $((TOTAL_PROBES - 1)) ]] && RB_ARGS+=(--skip-teardown)
        [[ -n "${PROFILE_VALUES}" ]]        && RB_ARGS+=(--profile "${PROFILE_VALUES}")
        for set_arg in "${HELM_SET_ARGS[@]+"${HELM_SET_ARGS[@]}"}"; do
            RB_ARGS+=(--set "${set_arg}")
        done

        if ! "${SCRIPT_DIR}/run-benchmark.sh" "${RB_ARGS[@]}" \
                "${SCENARIO}" "${WORKLOAD}" "${PROBE_OUTPUT}"; then
            FAILED=true
            break
        fi
    done

    [[ "${FAILED}" == "true" ]] && echo "  Sweep stopped early for ${SCENARIO}" >&2
    echo ""
done

echo "=== Sweep complete ==="
echo "Results written to: ${OUTPUT_DIR}"
echo ""
print_summary
