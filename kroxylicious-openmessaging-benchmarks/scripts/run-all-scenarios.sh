#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Workloads to run for each scenario.
WORKLOADS=(1topic-1kb 10topics-1kb 100topics-1kb)

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [options] [<baseline> <candidate>] <output-dir>

Runs two benchmark scenarios across all workloads and produces a side-by-side
comparison to quantify overhead.

For each scenario/workload combination, run-benchmark.sh is called.
Each benchmark deploys fresh infrastructure, runs to completion, collects
results, then tears down before the next run starts.

After all runs complete, compare-results.sh is called for each workload.

Arguments:
  baseline   Baseline scenario name (default: baseline)
  candidate  Candidate scenario name (default: proxy-no-filters)
  output-dir Root directory for all results. Results are organised as:
               <output-dir>/<scenario>/<workload>/

Options:
  --cluster-overrides <file> Helm values file with cluster-specific settings (storage class,
                             images, resource limits). Applied after --profile files so cluster
                             settings always win. Passed through to each run-benchmark.sh call.
  --profile <values-file>    Additional Helm values file layered on top of each scenario.
                             May be specified multiple times; files are applied in order.
  -h, --help                 Show this help

Environment:
  NAMESPACE              Kubernetes namespace (default: kafka)
  KAFKA_READY_TIMEOUT    Timeout waiting for Kafka cluster (default: 600s)
  POD_READY_TIMEOUT      Timeout waiting for pods (default: 300s)

Examples:
  $(basename "$0") ./results/run-$(date +%Y%m%d-%H%M%S)/
  $(basename "$0") baseline encryption \
    --cluster-overrides ~/my-cluster.yaml ./results/run-$(date +%Y%m%d-%H%M%S)/
EOF
    exit 1
}

PROFILE_VALUES=()
CLUSTER_OVERRIDES=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cluster-overrides)
            CLUSTER_OVERRIDES="$2"
            shift 2
            ;;
        --profile)
            PROFILE_VALUES+=("$2")
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        -*)
            echo "Error: unknown option '$1'" >&2
            usage
            ;;
        *)
            break
            ;;
    esac
done

if [[ $# -eq 1 ]]; then
    BASELINE_SCENARIO="baseline"
    CANDIDATE_SCENARIO="proxy-no-filters"
    OUTPUT_DIR="$1"
elif [[ $# -eq 3 ]]; then
    BASELINE_SCENARIO="$1"
    CANDIDATE_SCENARIO="$2"
    OUTPUT_DIR="$3"
else
    echo "Error: expected 1 or 3 arguments, got $#" >&2
    usage
fi

SCENARIOS=("${BASELINE_SCENARIO}" "${CANDIDATE_SCENARIO}")

for profile_file in ${PROFILE_VALUES[@]+"${PROFILE_VALUES[@]}"}; do
    if [[ ! -f "${profile_file}" ]]; then
        echo "Error: profile values file not found: ${profile_file}" >&2
        exit 1
    fi
done

if [[ -n "${CLUSTER_OVERRIDES}" && ! -f "${CLUSTER_OVERRIDES}" ]]; then
    echo "Error: cluster-overrides file not found: ${CLUSTER_OVERRIDES}" >&2
    exit 1
fi

echo "=== Running benchmark scenarios ==="
echo "Baseline:  ${BASELINE_SCENARIO}"
echo "Candidate: ${CANDIDATE_SCENARIO}"
echo "Workloads: ${WORKLOADS[*]}"
echo "Output:    ${OUTPUT_DIR}"
if [[ ${#PROFILE_VALUES[@]} -gt 0 ]]; then
    echo "Profiles:  ${PROFILE_VALUES[*]}"
fi
if [[ -n "${CLUSTER_OVERRIDES}" ]]; then
    echo "Cluster:   ${CLUSTER_OVERRIDES}"
fi
echo ""

# --- Run all scenario/workload combinations ---

RUN_BENCHMARK_ARGS=()
for profile_file in ${PROFILE_VALUES[@]+"${PROFILE_VALUES[@]}"}; do RUN_BENCHMARK_ARGS+=(--profile "${profile_file}"); done
[[ -n "${CLUSTER_OVERRIDES}" ]] && RUN_BENCHMARK_ARGS+=(--cluster-overrides "${CLUSTER_OVERRIDES}")

for SCENARIO in "${SCENARIOS[@]}"; do
    for WORKLOAD in "${WORKLOADS[@]}"; do
        SCENARIO_OUTPUT="${OUTPUT_DIR}/${SCENARIO}/${WORKLOAD}"
        echo ">>> ${SCENARIO} / ${WORKLOAD}"
        # ${array[@]+"${array[@]}"} is the bash 3.2-safe idiom for expanding an array that may be
        # empty: the + operator expands to nothing when the array is unset or empty, avoiding the
        # "unbound variable" error that set -u raises for empty array expansions in bash 3.2.
        "${SCRIPT_DIR}/run-benchmark.sh" ${RUN_BENCHMARK_ARGS[@]+"${RUN_BENCHMARK_ARGS[@]}"} "${SCENARIO}" "${WORKLOAD}" "${SCENARIO_OUTPUT}"
        echo ""
    done
done

# --- Generate filtered sources for result tooling ---

echo "Generating result tooling sources..."
mvn -q process-sources -pl kroxylicious-openmessaging-benchmarks -f "${SCRIPT_DIR}/../../pom.xml"

# --- Compare baseline vs proxy-no-filters ---

echo "=== Comparing ${BASELINE_SCENARIO} vs ${CANDIDATE_SCENARIO} ==="
echo ""

FILTERED="${SCRIPT_DIR}/../target/jbang/generated-sources/io/kroxylicious/benchmarks/results/CompareResults.java"
if [[ ! -f "${FILTERED}" ]]; then
    echo "Warning: comparison tooling not available after mvn process-sources, skipping." >&2
else
    for WORKLOAD in "${WORKLOADS[@]}"; do
        # Filter out run-metadata.json — we want the OMB result files only
        BASELINE_OMB=()
        for f in "${OUTPUT_DIR}/${BASELINE_SCENARIO}/${WORKLOAD}/"*.json; do
            [[ -f "${f}" && "$(basename "$f")" != "run-metadata.json" ]] && BASELINE_OMB+=("${f}")
        done
        CANDIDATE_OMB=()
        for f in "${OUTPUT_DIR}/${CANDIDATE_SCENARIO}/${WORKLOAD}/"*.json; do
            [[ -f "${f}" && "$(basename "$f")" != "run-metadata.json" ]] && CANDIDATE_OMB+=("${f}")
        done

        if [[ ${#BASELINE_OMB[@]} -eq 0 || ${#CANDIDATE_OMB[@]} -eq 0 ]]; then
            echo "  ${WORKLOAD}: skipping (missing results for one or both scenarios)"
            continue
        fi

        echo "--- ${WORKLOAD}: ${BASELINE_SCENARIO} vs ${CANDIDATE_SCENARIO} ---"
        "${SCRIPT_DIR}/compare-results.sh" "${BASELINE_OMB[0]}" "${CANDIDATE_OMB[0]}"
        echo ""

        # Check all scenarios for producer back-pressure and suggest a rate sweep if detected.
        _BP_RESULTS=()
        for _bp_scenario in "${SCENARIOS[@]}"; do
            for _bp_f in "${OUTPUT_DIR}/${_bp_scenario}/${WORKLOAD}/"*.json; do
                [[ -f "${_bp_f}" && "$(basename "${_bp_f}")" != "run-metadata.json" ]] && _BP_RESULTS+=("${_bp_f}")
            done
        done
        if [[ ${#_BP_RESULTS[@]} -gt 0 ]]; then
            "${SCRIPT_DIR}/check-backpressure.sh" --workload "${WORKLOAD}" "${_BP_RESULTS[@]}" || true
            echo ""
        fi
    done
fi

echo "=== All scenarios complete ==="
echo "Results written to: ${OUTPUT_DIR}"