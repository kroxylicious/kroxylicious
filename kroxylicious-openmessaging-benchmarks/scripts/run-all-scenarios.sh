#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Scenarios to run in order. Results are compared pairwise: each scenario vs baseline.
SCENARIOS=(baseline proxy-no-filters)

# Workloads to run for each scenario.
WORKLOADS=(1topic-1kb 10topics-1kb 100topics-1kb)

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") <output-dir>

Runs the baseline and proxy-no-filters scenarios across all workloads and
produces a side-by-side comparison to quantify proxy overhead.

For each scenario/workload combination, run-benchmark.sh is called.
Each benchmark deploys fresh infrastructure, runs to completion, collects
results, then tears down before the next run starts.

After all runs complete, compare-results.sh is called for each workload
to produce a baseline vs proxy-no-filters comparison.

Arguments:
  output-dir  Root directory for all results. Results are organised as:
                <output-dir>/<scenario>/<workload>/

Options:
  -h, --help  Show this help

Environment:
  NAMESPACE              Kubernetes namespace (default: kafka)
  KAFKA_READY_TIMEOUT    Timeout waiting for Kafka cluster (default: 600s)
  POD_READY_TIMEOUT      Timeout waiting for pods (default: 300s)

Examples:
  $(basename "$0") ./results/run-$(date +%Y%m%d-%H%M%S)/
EOF
    exit 1
}

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
    usage
fi

if [[ $# -ne 1 ]]; then
    echo "Error: expected 1 argument, got $#" >&2
    usage
fi

OUTPUT_DIR="$1"

echo "=== Running all benchmark scenarios ==="
echo "Scenarios: ${SCENARIOS[*]}"
echo "Workloads: ${WORKLOADS[*]}"
echo "Output:    ${OUTPUT_DIR}"
echo ""

# --- Run all scenario/workload combinations ---

for SCENARIO in "${SCENARIOS[@]}"; do
    for WORKLOAD in "${WORKLOADS[@]}"; do
        SCENARIO_OUTPUT="${OUTPUT_DIR}/${SCENARIO}/${WORKLOAD}"
        echo ">>> ${SCENARIO} / ${WORKLOAD}"
        "${SCRIPT_DIR}/run-benchmark.sh" "${SCENARIO}" "${WORKLOAD}" "${SCENARIO_OUTPUT}"
        echo ""
    done
done

# --- Compare baseline vs proxy-no-filters ---

echo "=== Comparing baseline vs proxy-no-filters ==="
echo ""

FILTERED="${SCRIPT_DIR}/../target/jbang/generated-sources/io/kroxylicious/benchmarks/results/CompareResults.java"
if [[ ! -f "${FILTERED}" ]]; then
    echo "Skipping comparison: run 'mvn process-sources -pl kroxylicious-openmessaging-benchmarks' to enable it." >&2
else
    for WORKLOAD in "${WORKLOADS[@]}"; do
        BASELINE_RESULTS=("${OUTPUT_DIR}/baseline/${WORKLOAD}/"*.json)
        PROXY_RESULTS=("${OUTPUT_DIR}/proxy-no-filters/${WORKLOAD}/"*.json)

        # Filter out run-metadata.json - we want the OMB result files only
        BASELINE_OMB=()
        for f in "${BASELINE_RESULTS[@]}"; do
            [[ "$(basename "$f")" != "run-metadata.json" ]] && BASELINE_OMB+=("$f")
        done
        PROXY_OMB=()
        for f in "${PROXY_RESULTS[@]}"; do
            [[ "$(basename "$f")" != "run-metadata.json" ]] && PROXY_OMB+=("$f")
        done

        if [[ ${#BASELINE_OMB[@]} -eq 0 || ${#PROXY_OMB[@]} -eq 0 ]]; then
            echo "  ${WORKLOAD}: skipping (missing results for one or both scenarios)"
            continue
        fi

        echo "--- ${WORKLOAD}: baseline vs proxy-no-filters ---"
        "${SCRIPT_DIR}/compare-results.sh" "${BASELINE_OMB[0]}" "${PROXY_OMB[0]}"
        echo ""
    done
fi

echo "=== All scenarios complete ==="
echo "Results written to: ${OUTPUT_DIR}"