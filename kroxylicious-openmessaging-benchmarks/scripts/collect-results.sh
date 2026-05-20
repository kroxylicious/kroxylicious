#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODULE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FILTERED="${MODULE_DIR}/target/jbang/generated-sources/io/kroxylicious/benchmarks/results/cli/CollectResults.java"

NAMESPACE="${NAMESPACE:-kafka}"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [options] <output-dir>

Collects JFR recording and flamegraph from the proxy pod and generates run metadata.
Result JSON (result.json) must already be present in output-dir — use collect_result_from_pvc
in run-benchmark.sh or retrieve it from the results PVC directly.

Arguments:
  output-dir                Directory containing result.json, to write JFR/flamegraph into

Options:
  --scenario <name>                   Benchmark scenario name written into run-metadata.json
  --workload <name>                   OMB workload name written into run-metadata.json
  --target-rate <n>                   Target producer rate (msg/sec) written into run-metadata.json
  --warmup-duration-minutes <n>       Warmup phase duration in minutes
  --test-duration-minutes <n>         Measurement phase duration in minutes
  --benchmark-started-at <iso8601>    UTC timestamp when the benchmark job started
  --benchmark-completed-at <iso8601>  UTC timestamp when the benchmark job completed
  --topics <n>                        Number of topics in the workload
  --partitions-per-topic <n>          Number of partitions per topic
  --message-size <n>                  Message payload size in bytes
  --producers-per-topic <n>           Number of producers per topic
  --consumer-per-subscription <n>     Number of consumers per subscription

Environment:
  NAMESPACE                 Kubernetes namespace (default: kafka)
  PROXY_POD                 Name of the proxy pod to collect JFR from (set by run-benchmark.sh)

Prerequisites:
  - kubectl configured with access to the cluster
  - mvn process-sources run to generate filtered JBang sources
  - result.json already present in output-dir
EOF
    exit 1
}

SCENARIO=""
WORKLOAD=""
TARGET_RATE=""
WARMUP_DURATION_MINUTES=""
TEST_DURATION_MINUTES=""
BENCHMARK_STARTED_AT=""
BENCHMARK_COMPLETED_AT=""
TOPICS=""
PARTITIONS_PER_TOPIC=""
MESSAGE_SIZE=""
PRODUCERS_PER_TOPIC=""
CONSUMER_PER_SUBSCRIPTION=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            ;;
        --scenario)                  SCENARIO="$2";                  shift 2 ;;
        --workload)                  WORKLOAD="$2";                  shift 2 ;;
        --target-rate)               TARGET_RATE="$2";               shift 2 ;;
        --warmup-duration-minutes)   WARMUP_DURATION_MINUTES="$2";   shift 2 ;;
        --test-duration-minutes)     TEST_DURATION_MINUTES="$2";     shift 2 ;;
        --benchmark-started-at)      BENCHMARK_STARTED_AT="$2";      shift 2 ;;
        --benchmark-completed-at)    BENCHMARK_COMPLETED_AT="$2";    shift 2 ;;
        --topics)                    TOPICS="$2";                    shift 2 ;;
        --partitions-per-topic)      PARTITIONS_PER_TOPIC="$2";      shift 2 ;;
        --message-size)              MESSAGE_SIZE="$2";              shift 2 ;;
        --producers-per-topic)       PRODUCERS_PER_TOPIC="$2";       shift 2 ;;
        --consumer-per-subscription) CONSUMER_PER_SUBSCRIPTION="$2"; shift 2 ;;
        -*)
            echo "Error: unknown option $1" >&2
            usage
            ;;
        *)
            break
            ;;
    esac
done

if [[ $# -ne 1 ]]; then
    usage
fi

OUTPUT_DIR="$1"

if [[ ! -f "$FILTERED" ]]; then
    echo "Error: run 'mvn process-sources -pl kroxylicious-openmessaging-benchmarks' first to generate filtered sources" >&2
    exit 1
fi

if [[ ! -f "$OUTPUT_DIR/result.json" ]]; then
    echo "Error: result.json not found in $OUTPUT_DIR — collect it from the results PVC first" >&2
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Copy JFR recording and flamegraph directly from the proxy pod while it is still running.
# run-benchmark.sh calls jcmd JFR.stop before invoking this script, so the files are
# already written to /tmp inside the pod's emptyDir volume.
PROXY_POD="${PROXY_POD:-}"
JFR_FILE="/tmp/benchmark.jfr"
FLAMEGRAPH_FILE="/tmp/flamegraph.html"

if [[ -n "${PROXY_POD}" ]] && kubectl get pod "${PROXY_POD}" -n "${NAMESPACE}" &>/dev/null; then
    echo "Copying JFR recording from proxy pod ${PROXY_POD}..."
    # Use 'kubectl exec -- cat' rather than 'kubectl cp' — the proxy container does not
    # have tar, which kubectl cp requires.
    kubectl exec -n "${NAMESPACE}" "${PROXY_POD}" -- cat "${JFR_FILE}" > "${OUTPUT_DIR}/benchmark.jfr" 2>/dev/null || true
    if [[ ! -s "${OUTPUT_DIR}/benchmark.jfr" ]]; then
        echo "Warning: benchmark.jfr is empty — JFR dump may not have completed" >&2
    else
        echo "  benchmark.jfr ($(du -h "${OUTPUT_DIR}/benchmark.jfr" | cut -f1))"
    fi
    if kubectl exec -n "${NAMESPACE}" "${PROXY_POD}" -- test -s "${FLAMEGRAPH_FILE}" 2>/dev/null; then
        kubectl exec -n "${NAMESPACE}" "${PROXY_POD}" -- cat "${FLAMEGRAPH_FILE}" > "${OUTPUT_DIR}/flamegraph.html" 2>/dev/null || true
        echo "  flamegraph.html ($(du -h "${OUTPUT_DIR}/flamegraph.html" | cut -f1))"
    else
        echo "Warning: flamegraph.html is absent or empty — async-profiler may not have run or perf events were unavailable" >&2
    fi
fi

# Generate run metadata
echo "Generating run metadata..."
JBANG_ARGS=(--generate-run-metadata "$OUTPUT_DIR")
[[ -n "${SCENARIO}" ]]                  && JBANG_ARGS+=(--scenario "${SCENARIO}")
[[ -n "${WORKLOAD}" ]]                  && JBANG_ARGS+=(--workload "${WORKLOAD}")
[[ -n "${TARGET_RATE}" ]]               && JBANG_ARGS+=(--target-rate "${TARGET_RATE}")
[[ -n "${WARMUP_DURATION_MINUTES}" ]]   && JBANG_ARGS+=(--warmup-duration-minutes "${WARMUP_DURATION_MINUTES}")
[[ -n "${TEST_DURATION_MINUTES}" ]]     && JBANG_ARGS+=(--test-duration-minutes "${TEST_DURATION_MINUTES}")
[[ -n "${BENCHMARK_STARTED_AT}" ]]      && JBANG_ARGS+=(--benchmark-started-at "${BENCHMARK_STARTED_AT}")
[[ -n "${BENCHMARK_COMPLETED_AT}" ]]    && JBANG_ARGS+=(--benchmark-completed-at "${BENCHMARK_COMPLETED_AT}")
[[ -n "${TOPICS}" ]]                    && JBANG_ARGS+=(--topics "${TOPICS}")
[[ -n "${PARTITIONS_PER_TOPIC}" ]]      && JBANG_ARGS+=(--partitions-per-topic "${PARTITIONS_PER_TOPIC}")
[[ -n "${MESSAGE_SIZE}" ]]              && JBANG_ARGS+=(--message-size "${MESSAGE_SIZE}")
[[ -n "${PRODUCERS_PER_TOPIC}" ]]       && JBANG_ARGS+=(--producers-per-topic "${PRODUCERS_PER_TOPIC}")
[[ -n "${CONSUMER_PER_SUBSCRIPTION}" ]] && JBANG_ARGS+=(--consumer-per-subscription "${CONSUMER_PER_SUBSCRIPTION}")
[[ -n "${PROXY_POD}" ]]                 && JBANG_ARGS+=(--proxy-pod "${PROXY_POD}" --namespace "${NAMESPACE}")
jbang "$FILTERED" "${JBANG_ARGS[@]}"

echo "Done. Results collected in $OUTPUT_DIR/"
