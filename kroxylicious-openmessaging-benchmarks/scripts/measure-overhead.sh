#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

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

echo "Not yet implemented — use --dry-run to preview the rate sequence" >&2
exit 1