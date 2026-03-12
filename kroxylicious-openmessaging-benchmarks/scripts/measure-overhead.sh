#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
HELM_CHART="${MODULE_DIR}/helm/kroxylicious-benchmark"
HELM_RELEASE="benchmark"

# Workload to use for all probes. Controls topic count and message size.
# Rate is overridden per-probe; all other workload settings come from this template.
WORKLOAD="1topic-1kb"

NAMESPACE="${NAMESPACE:-kafka}"
KAFKA_READY_TIMEOUT="${KAFKA_READY_TIMEOUT:-600s}"
POD_READY_TIMEOUT="${POD_READY_TIMEOUT:-300s}"
# Maximum time to wait for a single OMB probe to complete (seconds).
# Should exceed testDurationMinutes + warmupDurationMinutes + setup overhead.
# Default: 30 minutes, suitable for smoke runs. Increase for production runs.
PROBE_TIMEOUT="${PROBE_TIMEOUT:-1800}"

DEFAULT_SCENARIOS="baseline,proxy-no-filters"
DEFAULT_STEP_PERCENT=10

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [options] --output-dir <dir>

Runs a rate sweep across one or more scenarios to measure proxy overhead
at different throughput levels. For each scenario, infrastructure is deployed
once, OMB is run at each rate in the sweep, then infrastructure is torn down.

Results are written as JSON per rate step. A summary table is printed at the
end. If baseline results are available (from this run or via --baseline-from),
a side-by-side comparison is produced.

Options:
  --output-dir <dir>        Directory to write results into (required)
  --scenarios <list>        Comma-separated scenarios to run
                            (default: ${DEFAULT_SCENARIOS})
  --baseline-from <dir>     Read pre-existing baseline results from here;
                            implies baseline is excluded from --scenarios
  --min-rate <n>            Minimum producer rate in msg/sec (required)
                            Suggested: ~10% of the cluster's expected maximum throughput
  --max-rate <n>            Maximum producer rate in msg/sec (required)
                            Suggested: ~120% of expected maximum to ensure saturation is reached
  --step-percent <n>        Step size as a percentage of the range (default: ${DEFAULT_STEP_PERCENT})
                            Fixed increment: (max - min) * step% — e.g. step=25% gives 4 probes
  --profile <values-file>   Additional Helm values layered on top of each scenario
  --dry-run                 Print rate sequence and planned steps without deploying anything
  -h, --help                Show this help

Environment:
  NAMESPACE              Kubernetes namespace (default: kafka)
  KAFKA_READY_TIMEOUT    Timeout waiting for Kafka cluster (default: 600s)
  POD_READY_TIMEOUT      Timeout waiting for pods (default: 300s)
  PROBE_TIMEOUT          Max seconds to wait for a single probe to complete (default: 1800)
                         Increase for production runs (testDuration + warmupDuration + overhead)

Examples:
  # Full run — baseline then proxy, 10% steps from 10k to 110k msg/sec
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
PROFILE_VALUES=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir)    OUTPUT_DIR="$2";     shift 2 ;;
        --scenarios)     SCENARIOS="$2";      shift 2 ;;
        --baseline-from) BASELINE_FROM="$2";  shift 2 ;;
        --min-rate)      MIN_RATE="$2";       shift 2 ;;
        --max-rate)      MAX_RATE="$2";       shift 2 ;;
        --step-percent)  STEP_PERCENT="$2";   shift 2 ;;
        --profile)       PROFILE_VALUES="$2"; shift 2 ;;
        --dry-run)       DRY_RUN=true;        shift   ;;
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

if [[ "${MAX_RATE}" -le "${MIN_RATE}" ]]; then
    echo "Error: --max-rate (${MAX_RATE}) must be greater than --min-rate (${MIN_RATE})" >&2
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
# Prints one integer rate per line.
rate_sequence() {
    awk -v min="${MIN_RATE}" -v max="${MAX_RATE}" -v step="${STEP_PERCENT}" 'BEGIN {
        increment = (max - min) * step / 100
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
    echo "Output dir:   ${OUTPUT_DIR}"
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

# --- Infrastructure lifecycle ---

deploy_scenario() {
    local scenario="$1"
    local scenario_values="${HELM_CHART}/scenarios/${scenario}-values.yaml"

    if [[ ! -f "${scenario_values}" ]]; then
        echo "Error: scenario values not found: ${scenario_values}" >&2
        exit 1
    fi

    echo "--- Deploying ${scenario} ---"
    local helm_args=(-n "${NAMESPACE}" -f "${scenario_values}" --set omb.workload="${WORKLOAD}")
    # ${array[@]+"${array[@]}"} is the bash 3.2-safe idiom for empty array expansion (see run-all-scenarios.sh)
    [[ -n "${PROFILE_VALUES}" ]] && helm_args+=(-f "${PROFILE_VALUES}")
    helm install "${HELM_RELEASE}" "${HELM_CHART}" "${helm_args[@]}"

    echo "Waiting for Kafka (timeout: ${KAFKA_READY_TIMEOUT})..."
    kubectl wait kafka/kafka --for=condition=Ready --timeout="${KAFKA_READY_TIMEOUT}" -n "${NAMESPACE}"

    echo "Waiting for OMB workers..."
    kubectl rollout status statefulset/omb-worker -n "${NAMESPACE}" --timeout="${POD_READY_TIMEOUT}"

    echo "Waiting for OMB benchmark pod..."
    kubectl wait --for=condition=ready pod -l app=omb-benchmark -n "${NAMESPACE}" --timeout="${POD_READY_TIMEOUT}"

    echo "Infrastructure ready."
}

teardown_scenario() {
    echo "--- Tearing down ---"
    if helm status "${HELM_RELEASE}" -n "${NAMESPACE}" &>/dev/null; then
        helm uninstall "${HELM_RELEASE}" -n "${NAMESPACE}"
    fi
    kubectl delete pvc -l strimzi.io/cluster=kafka -n "${NAMESPACE}" --ignore-not-found
}

# --- Probe execution ---

# Waits for all OMB worker pods to be ready before starting a probe.
# A bounced worker pod causes the OMB JVM to hang in shutdown (UnknownHostException
# in stopAll()), which would cause kubectl exec to hang indefinitely.
# Grants a grace period of POD_READY_TIMEOUT for workers to recover after a bounce
# rather than immediately failing the probe.
check_workers_healthy() {
    local desired
    desired=$(kubectl get statefulset omb-worker -n "${NAMESPACE}" \
        -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
    if [[ "${desired}" == "0" ]]; then
        echo "  ✗ OMB worker StatefulSet not found or has 0 replicas" >&2
        return 1
    fi

    local ready
    ready=$(kubectl get statefulset omb-worker -n "${NAMESPACE}" \
        -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
    if [[ "${ready}" == "${desired}" ]]; then
        return 0
    fi

    echo "  OMB workers not fully ready (${ready}/${desired}) — waiting up to ${POD_READY_TIMEOUT}..."
    if kubectl rollout status statefulset/omb-worker \
            -n "${NAMESPACE}" --timeout="${POD_READY_TIMEOUT}" 2>/dev/null; then
        echo "  OMB workers ready."
    else
        echo "  ✗ OMB workers did not recover within ${POD_READY_TIMEOUT}" >&2
        echo "    kubectl get pods -l app=omb-worker -n ${NAMESPACE}" >&2
        return 1
    fi
}

run_probe() {
    local rate="$1"
    local probe_output_dir="$2"

    mkdir -p "${probe_output_dir}"
    local omb_log="${probe_output_dir}/omb.log"

    # Verify workers are healthy before starting — a bounced worker pod causes the
    # OMB JVM to hang in stopAll() with UnknownHostException, which would cause
    # kubectl exec to block indefinitely.
    if ! check_workers_healthy; then
        return 1
    fi

    # Clear previous OMB result files from the pod so we can unambiguously
    # identify the result from this probe afterwards.
    kubectl exec deploy/omb-benchmark -n "${NAMESPACE}" -- \
        sh -c 'rm -f /var/lib/omb/results/*.json' 2>/dev/null || true

    # Run OMB with the target producer rate. The workload ConfigMap is read-only,
    # so we use sed to write a modified copy into /tmp and run from there.
    # OMB output is redirected to a log file to keep the terminal readable.
    # timeout guards against the OMB JVM hanging in shutdown (e.g. after a worker
    # pod is bounced mid-run); exit code 124 means the timeout was hit.
    echo "  OMB log: ${omb_log}"
    local exec_rc=0
    timeout "${PROBE_TIMEOUT}" \
        kubectl exec deploy/omb-benchmark -n "${NAMESPACE}" -- \
        sh -c "sed 's/^producerRate:.*/producerRate: ${rate}/' /etc/omb/workloads/workload.yaml > /tmp/workload.yaml && \
               cd /var/lib/omb/results && \
               /opt/benchmark/bin/benchmark \
                 --drivers /etc/omb/driver/driver-kafka.yaml \
                 --workers \"\${WORKERS}\" \
                 /tmp/workload.yaml" > "${omb_log}" 2>&1 || exec_rc=$?

    if [[ "${exec_rc}" -eq 124 ]]; then
        echo "  TIMED OUT after ${PROBE_TIMEOUT}s — OMB JVM may be stuck in shutdown" >&2
        echo "  Last lines of OMB log:" >&2
        tail -10 "${omb_log}" >&2 || true
        echo "  Consider increasing PROBE_TIMEOUT (current: ${PROBE_TIMEOUT}s)" >&2
        return 1
    elif [[ "${exec_rc}" -ne 0 ]]; then
        echo "  FAILED (exit ${exec_rc})" >&2
        echo "  Last lines of OMB log:" >&2
        tail -5 "${omb_log}" >&2 || true
        diagnose_pod_failure "app=omb-benchmark" "${rate}"
        return 1
    fi

    # Collect the result JSON produced by this probe.
    local benchmark_pod
    benchmark_pod=$(kubectl get pod -n "${NAMESPACE}" -l app=omb-benchmark \
        -o jsonpath='{.items[0].metadata.name}')

    local result_file
    result_file=$(kubectl exec -n "${NAMESPACE}" "${benchmark_pod}" -- \
        sh -c 'ls -t /var/lib/omb/results/*.json 2>/dev/null | head -1') || true

    if [[ -z "${result_file}" ]]; then
        echo "  No result file found — OMB may not have written output" >&2
        diagnose_pod_failure "app=omb-benchmark" "${rate}"
        return 1
    fi

    kubectl cp "${NAMESPACE}/${benchmark_pod}:${result_file}" "${probe_output_dir}/result.json"
    echo "  Result collected: ${probe_output_dir}/result.json"
}

# Queries pod status after a failure and emits a human-readable diagnosis.
# Distinguishes OOMKill / eviction (infrastructure limit) from OMB process errors.
diagnose_pod_failure() {
    local label="$1"
    local rate="$2"

    local phase reason restart_count
    phase=$(kubectl get pod -n "${NAMESPACE}" -l "${label}" \
        -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "Unknown")
    reason=$(kubectl get pod -n "${NAMESPACE}" -l "${label}" \
        -o jsonpath='{.items[0].status.containerStatuses[0].lastState.terminated.reason}' \
        2>/dev/null || echo "")
    restart_count=$(kubectl get pod -n "${NAMESPACE}" -l "${label}" \
        -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' \
        2>/dev/null || echo "0")

    if [[ "${reason}" == "OOMKilled" ]]; then
        echo "  ✗ OOMKilled at ${rate} msg/sec — pod exceeded its memory limit" >&2
        echo "    Increase memory limits or reduce the target rate" >&2
    elif [[ "${phase}" != "Running" ]]; then
        echo "  ✗ Pod is not Running (phase=${phase} reason=${reason} restarts=${restart_count})" >&2
        echo "    kubectl describe pod -l ${label} -n ${NAMESPACE}" >&2
    elif [[ "${restart_count}" -gt 0 ]]; then
        echo "  ✗ Pod restarted during probe (restarts=${restart_count} last reason=${reason})" >&2
        echo "    kubectl logs -l ${label} -n ${NAMESPACE} --previous" >&2
    else
        echo "  ✗ OMB process exited non-zero — benchmark may have failed" >&2
        echo "    kubectl logs -l ${label} -n ${NAMESPACE} | tail -50" >&2
    fi
}

# --- Summary table ---

# Prints a rate-by-rate comparison table across all scenarios.
# For each rate step shows: achieved msg/s (to detect saturation) and
# mean end-to-end p99 latency. If baseline results are present an
# overhead column is appended for each non-baseline scenario.
print_summary() {
    if ! command -v jq &>/dev/null; then
        echo "Warning: jq not found — skipping summary table" >&2
        return
    fi

    # Resolve baseline results directory (from --baseline-from or this run)
    local baseline_dir=""
    if [[ -n "${BASELINE_FROM}" ]]; then
        baseline_dir="${BASELINE_FROM}"
    elif [[ -d "${OUTPUT_DIR}/baseline" ]]; then
        baseline_dir="${OUTPUT_DIR}/baseline"
    fi

    # Collect probe rates from the reference directory, sorted numerically
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

    # Non-baseline scenarios to compare against baseline
    local other_scenarios=()
    for s in "${SCENARIO_ARRAY[@]}"; do
        [[ "${s}" != "baseline" ]] && other_scenarios+=("${s}")
    done

    echo "=== Summary ==="
    echo "(achieved = mean publish rate; p99 = mean end-to-end latency p99; ✗ = saturated)"
    echo ""

    # Header
    printf "%-14s" "Target (msg/s)"
    if [[ -n "${baseline_dir}" ]]; then
        printf "  %-20s  %-14s" "Baseline achieved" "Baseline p99"
    fi
    for s in "${other_scenarios[@]}"; do
        printf "  %-20s  %-14s" "${s} achieved" "${s} p99"
        [[ -n "${baseline_dir}" ]] && printf "  %-14s" "Overhead"
    done
    printf "\n"
    printf '%s\n' "$(printf '%*s' 100 '' | tr ' ' '-')"

    # One row per rate
    for rate in "${rates[@]}"; do
        printf "%-14s" "$(printf '%'"'"'d' "${rate}")"

        # Baseline columns
        local baseline_p99=""
        if [[ -n "${baseline_dir}" ]]; then
            local bf="${baseline_dir}/rate-${rate}/result.json"
            local bd="${baseline_dir}/rate-${rate}"
            if [[ -f "${bf}" ]]; then
                local b_achieved b_sat b_p99
                b_achieved=$(jq '[.publishRate[]] | add / length' "${bf}")
                b_sat=$(awk "BEGIN { print (${b_achieved} < ${rate} * 0.95) ? 1 : 0 }")
                if [[ "${b_sat}" == "1" ]]; then
                    printf "  %-20s  %-14s" "✗ saturated" "—"
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

        # Non-baseline scenario columns
        for s in "${other_scenarios[@]}"; do
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
                printf "  %-20s  %-14s" "✗ saturated" "—"
                [[ -n "${baseline_dir}" ]] && printf "  %-14s" "—"
            else
                s_p99=$(jq '[.endToEndLatency99pct[]] | add / length' "${sf}")
                printf "  %-20s  %-14s" \
                    "$(printf '%.0f' "${s_achieved}")" \
                    "$(printf '%.2f ms' "${s_p99}")"
                if [[ -n "${baseline_dir}" ]]; then
                    if [[ -n "${baseline_p99}" ]]; then
                        local overhead
                        overhead=$(awk "BEGIN { printf \"+%.2f ms\", ${s_p99} - ${baseline_p99} }")
                        printf "  %-14s" "${overhead}"
                    else
                        printf "  %-14s" "—"
                    fi
                fi
            fi
        done
        printf "\n"
    done
    echo ""
}

# --- Main ---

trap teardown_scenario EXIT

# If --baseline-from is provided, exclude baseline from the run list —
# the pre-existing results will be used for comparison instead.
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

echo "=== Measuring proxy overhead ==="
echo "Scenarios:  ${SCENARIO_LIST}"
echo "Output dir: ${OUTPUT_DIR}"
echo "Rates:      ${MIN_RATE}–${MAX_RATE} msg/sec (${STEP_PERCENT}% steps)"
[[ -n "${BASELINE_FROM}" ]] && echo "Baseline:   ${BASELINE_FROM}"
echo ""

for SCENARIO in "${SCENARIO_ARRAY[@]}"; do
    SCENARIO_OUTPUT="${OUTPUT_DIR}/${SCENARIO}"

    echo "=== Scenario: ${SCENARIO} ==="

    if helm status "${HELM_RELEASE}" -n "${NAMESPACE}" &>/dev/null; then
        echo "Warning: Helm release '${HELM_RELEASE}' already exists, tearing down first."
        teardown_scenario
    fi

    deploy_scenario "${SCENARIO}"

    # Pre-collect rates so we can show N/M progress
    RATES=()
    while IFS= read -r r; do RATES+=("$r"); done < <(rate_sequence)
    TOTAL_PROBES=${#RATES[@]}

    FAILED=false
    for i in "${!RATES[@]}"; do
        RATE="${RATES[$i]}"
        PROBE_NUM=$((i + 1))
        PROBE_OUTPUT="${SCENARIO_OUTPUT}/rate-${RATE}"
        echo "--- Probe ${PROBE_NUM}/${TOTAL_PROBES}: $(printf '%'"'"'d' "${RATE}") msg/sec ---"
        if ! run_probe "${RATE}" "${PROBE_OUTPUT}"; then
            FAILED=true
            break
        fi
    done
    [[ "${FAILED}" == "true" ]] && echo "  Sweep stopped early for ${SCENARIO}" >&2

    teardown_scenario
    echo ""
done

echo "=== Sweep complete ==="
echo "Results written to: ${OUTPUT_DIR}"
echo ""
print_summary