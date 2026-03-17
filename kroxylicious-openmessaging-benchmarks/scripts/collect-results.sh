#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODULE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FILTERED="${MODULE_DIR}/target/jbang/generated-sources/io/kroxylicious/benchmarks/results/CollectResults.java"

NAMESPACE="${NAMESPACE:-kafka}"
BENCHMARK_POD_LABEL="app=omb-benchmark"
BENCHMARK_RESULTS_DIR="/var/lib/omb/results"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [--results-from <path>] <output-dir>

Collects OMB benchmark results from the benchmark pod and generates
run metadata.

Arguments:
  output-dir                Directory to write results and metadata into

Options:
  --results-from <path>      Directory on the pod to copy results from (default: /var/lib/omb/results)

Environment:
  NAMESPACE                 Kubernetes namespace (default: kafka)
  JFR_PVC_NAME              Name of the PVC holding the JFR recording (set by run-benchmark.sh)

Prerequisites:
  - kubectl configured with access to the cluster
  - mvn process-sources run to generate filtered JBang sources
  - A benchmark run completed in the benchmark pod
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --results-from)
            BENCHMARK_RESULTS_DIR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
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

# Find the benchmark pod
POD=$(kubectl get pod -n "$NAMESPACE" -l "$BENCHMARK_POD_LABEL" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true
if [[ -z "$POD" ]]; then
    echo "Error: no benchmark pod found with label $BENCHMARK_POD_LABEL in namespace $NAMESPACE" >&2
    exit 1
fi
echo "Found benchmark pod: $POD"

# List result files on the pod (use ls+grep for portability — find -printf is GNU-only)
RESULT_FILES=$(kubectl exec -n "$NAMESPACE" "$POD" -- sh -c "ls \"$BENCHMARK_RESULTS_DIR\"/*.json 2>/dev/null" | xargs -n1 basename 2>/dev/null) || true
if [[ -z "$RESULT_FILES" ]]; then
    echo "Error: no result JSON files found in $BENCHMARK_RESULTS_DIR on pod $POD" >&2
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Copy each result file
echo "Copying results to $OUTPUT_DIR/"
while IFS= read -r file; do
    echo "  $file"
    kubectl cp "$NAMESPACE/$POD:$BENCHMARK_RESULTS_DIR/$file" "$OUTPUT_DIR/$file"
done <<< "$RESULT_FILES"

# Copy JFR recording if one was created by run-benchmark.sh.
# Prefer copying directly from the proxy pod (still running at this point) to avoid
# EBS ReadWriteOnce attachment conflicts: a jfr-collect pod may land on a different
# node and block waiting for the volume to detach from the proxy node.
# Fall back to a jfr-collect pod only when the proxy pod is no longer available.
JFR_PVC_NAME="${JFR_PVC_NAME:-}"
PROXY_POD="${PROXY_POD:-}"
JFR_FILE="/tmp/benchmark.jfr"
FLAMEGRAPH_FILE="/tmp/flamegraph.html"

copy_jfr_files() {
    local src_pod="$1"
    # Use 'kubectl exec -- cat' rather than 'kubectl cp' — the proxy container does not
    # have tar, which kubectl cp requires.  Streaming via cat works for any container.
    kubectl exec -n "${NAMESPACE}" "${src_pod}" -- cat "${JFR_FILE}" > "${OUTPUT_DIR}/benchmark.jfr" 2>/dev/null || true
    if [[ ! -s "${OUTPUT_DIR}/benchmark.jfr" ]]; then
        echo "Warning: benchmark.jfr is empty — JFR dump may not have completed before pod terminated" >&2
    else
        echo "  benchmark.jfr ($(du -h "${OUTPUT_DIR}/benchmark.jfr" | cut -f1))"
    fi
    if kubectl exec -n "${NAMESPACE}" "${src_pod}" -- test -s "${FLAMEGRAPH_FILE}" 2>/dev/null; then
        kubectl exec -n "${NAMESPACE}" "${src_pod}" -- cat "${FLAMEGRAPH_FILE}" > "${OUTPUT_DIR}/flamegraph.html" 2>/dev/null || true
        echo "  flamegraph.html ($(du -h "${OUTPUT_DIR}/flamegraph.html" | cut -f1))"
    else
        echo "Warning: flamegraph.html is absent or empty — async-profiler may not have run or perf events were unavailable" >&2
    fi
}

if [[ -n "${PROXY_POD}" ]] && kubectl get pod "${PROXY_POD}" -n "${NAMESPACE}" &>/dev/null; then
    echo "Copying JFR recording directly from proxy pod ${PROXY_POD}..."
    copy_jfr_files "${PROXY_POD}"
elif [[ -n "${JFR_PVC_NAME}" ]] && kubectl get pvc "${JFR_PVC_NAME}" -n "${NAMESPACE}" &>/dev/null; then
    echo "Proxy pod gone — copying JFR recording from PVC ${JFR_PVC_NAME} via collector pod..."
    DEBUG_POD="jfr-collect-$$"
    delete_debug_pod() {
        kubectl delete pod "${DEBUG_POD}" -n "${NAMESPACE}" --ignore-not-found --wait=false 2>/dev/null || true
    }
    trap delete_debug_pod EXIT
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${DEBUG_POD}
  namespace: ${NAMESPACE}
  labels:
    app: jfr-collect
spec:
  restartPolicy: Never
  volumes:
  - name: jfr
    persistentVolumeClaim:
      claimName: ${JFR_PVC_NAME}
  containers:
  - name: debug
    image: busybox:1.37.0@sha256:1487d0af5f52b4ba31c7e465126ee2123fe3f2305d638e7827681e7cf6c83d5e
    command: ["sleep", "infinity"]
    volumeMounts:
    - name: jfr
      mountPath: /tmp
EOF
    kubectl wait pod "${DEBUG_POD}" -n "${NAMESPACE}" --for=condition=ready --timeout=120s
    copy_jfr_files "${DEBUG_POD}"
    # trap handles deletion on exit
fi

# Generate run metadata
echo "Generating run metadata..."
jbang "$FILTERED" --generate-run-metadata "$OUTPUT_DIR"

echo "Done. Results collected in $OUTPUT_DIR/"
