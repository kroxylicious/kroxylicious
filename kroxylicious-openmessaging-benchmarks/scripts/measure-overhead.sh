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

DEFAULT_SCENARIOS="baseline,proxy-no-filters"
DEFAULT_MIN_RATE=10000
DEFAULT_MAX_RATE=100000
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
  --min-rate <n>            Minimum producer rate in msg/sec (default: ${DEFAULT_MIN_RATE})
  --max-rate <n>            Maximum producer rate in msg/sec (default: ${DEFAULT_MAX_RATE})
  --step-percent <n>        Rate increase per step as a percentage (default: ${DEFAULT_STEP_PERCENT})
                            Each step: next_rate = current_rate * (1 + step_percent/100)
  --profile <values-file>   Additional Helm values layered on top of each scenario
  --dry-run                 Print rate sequence and planned steps without deploying anything
  -h, --help                Show this help

Environment:
  NAMESPACE              Kubernetes namespace (default: kafka)
  KAFKA_READY_TIMEOUT    Timeout waiting for Kafka cluster (default: 600s)
  POD_READY_TIMEOUT      Timeout waiting for pods (default: 300s)

Examples:
  # Full run — baseline then proxy, 10% steps from 10k to 100k msg/sec
  $(basename "$0") --output-dir ./results/run-1/

  # Preview rate sequence without deploying anything
  $(basename "$0") --dry-run --min-rate 10000 --max-rate 100000 --step-percent 50

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
MIN_RATE="${DEFAULT_MIN_RATE}"
MAX_RATE="${DEFAULT_MAX_RATE}"
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

if [[ -n "${BASELINE_FROM}" && ! -d "${BASELINE_FROM}" ]]; then
    echo "Error: --baseline-from directory not found: ${BASELINE_FROM}" >&2
    exit 1
fi

if [[ -n "${PROFILE_VALUES}" && ! -f "${PROFILE_VALUES}" ]]; then
    echo "Error: --profile file not found: ${PROFILE_VALUES}" >&2
    exit 1
fi

# --- Rate progression ---

# Computes the geometric rate sequence from MIN_RATE to MAX_RATE.
# Each step multiplies the previous rate by (1 + STEP_PERCENT/100).
# Prints one integer rate per line.
rate_sequence() {
    awk -v min="${MIN_RATE}" -v max="${MAX_RATE}" -v step="${STEP_PERCENT}" 'BEGIN {
        rate = min
        while (rate <= max) {
            printf "%d\n", rate
            next_rate = rate * (1 + step / 100)
            if (next_rate <= rate) {
                print "Error: step percent too small, rate not advancing" > "/dev/stderr"
                exit 1
            }
            rate = next_rate
        }
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
    echo "Rate sequence (min=${MIN_RATE}, max=${MAX_RATE}, step=${STEP_PERCENT}%):"
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

run_probe() {
    local rate="$1"
    local probe_output_dir="$2"

    mkdir -p "${probe_output_dir}"
    echo "  Probe at ${rate} msg/sec..."

    # Clear previous OMB result files from the pod so we can unambiguously
    # identify the result from this probe afterwards.
    kubectl exec deploy/omb-benchmark -n "${NAMESPACE}" -- \
        sh -c 'rm -f /var/lib/omb/results/*.json' 2>/dev/null || true

    # Run OMB with the target producer rate. The workload ConfigMap is read-only,
    # so we use sed to write a modified copy into /tmp and run from there.
    kubectl exec deploy/omb-benchmark -n "${NAMESPACE}" -- \
        sh -c "sed 's/^producerRate:.*/producerRate: ${rate}/' /etc/omb/workloads/workload.yaml > /tmp/workload.yaml && \
               cd /var/lib/omb/results && \
               /opt/benchmark/bin/benchmark \
                 --drivers /etc/omb/driver/driver-kafka.yaml \
                 --workers \"\${WORKERS}\" \
                 /tmp/workload.yaml"

    # Collect the result JSON produced by this probe.
    local benchmark_pod
    benchmark_pod=$(kubectl get pod -n "${NAMESPACE}" -l app=omb-benchmark \
        -o jsonpath='{.items[0].metadata.name}')

    local result_file
    result_file=$(kubectl exec -n "${NAMESPACE}" "${benchmark_pod}" -- \
        sh -c 'ls -t /var/lib/omb/results/*.json 2>/dev/null | head -1') || true

    if [[ -z "${result_file}" ]]; then
        echo "  Warning: no result file found for rate ${rate} msg/sec" >&2
        return 1
    fi

    kubectl cp "${NAMESPACE}/${benchmark_pod}:${result_file}" "${probe_output_dir}/result.json"
    echo "  Result collected: ${probe_output_dir}/result.json"
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

    while IFS= read -r RATE; do
        PROBE_OUTPUT="${SCENARIO_OUTPUT}/rate-${RATE}"
        if ! run_probe "${RATE}" "${PROBE_OUTPUT}"; then
            echo "  Probe failed at ${RATE} msg/sec — stopping sweep for ${SCENARIO}" >&2
            break
        fi
    done < <(rate_sequence)

    teardown_scenario
    echo ""
done

echo "=== Sweep complete ==="
echo "Results written to: ${OUTPUT_DIR}"