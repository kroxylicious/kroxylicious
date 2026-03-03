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

# Copy JFR recording from PVC if one was created by run-benchmark.sh
JFR_PVC_NAME="${JFR_PVC_NAME:-}"
JFR_FILE="/tmp/benchmark.jfr"

if [[ -n "${JFR_PVC_NAME}" ]] && kubectl get pvc "${JFR_PVC_NAME}" -n "${NAMESPACE}" &>/dev/null; then
    echo "Copying JFR recording from PVC ${JFR_PVC_NAME}..."
    DEBUG_POD="jfr-collect-$$"
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${DEBUG_POD}
  namespace: ${NAMESPACE}
spec:
  restartPolicy: Never
  volumes:
  - name: jfr
    persistentVolumeClaim:
      claimName: ${JFR_PVC_NAME}
  containers:
  - name: debug
    image: busybox:latest@sha256:b3255e7dfbcd10cb367af0d409747d511aeb66dfac98cf30e97e87e4207dd76f
    command: ["sleep", "infinity"]
    volumeMounts:
    - name: jfr
      mountPath: /tmp
EOF
    kubectl wait pod "${DEBUG_POD}" -n "${NAMESPACE}" --for=condition=ready --timeout=60s
    kubectl cp "${NAMESPACE}/${DEBUG_POD}:${JFR_FILE}" "${OUTPUT_DIR}/benchmark.jfr"
    kubectl delete pod "${DEBUG_POD}" -n "${NAMESPACE}" --ignore-not-found
    if [[ ! -s "${OUTPUT_DIR}/benchmark.jfr" ]]; then
        echo "Warning: benchmark.jfr is empty — JFR dump may not have completed before pod terminated" >&2
    else
        echo "  benchmark.jfr ($(du -h "${OUTPUT_DIR}/benchmark.jfr" | cut -f1))"
    fi
fi

# Generate run metadata
echo "Generating run metadata..."
jbang "$FILTERED" --generate-run-metadata "$OUTPUT_DIR"

echo "Done. Results collected in $OUTPUT_DIR/"
