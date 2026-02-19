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
BENCHMARK_RESULTS_DIR="/results"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") <output-dir>

Collects OMB benchmark results from the benchmark pod and generates
run metadata.

Arguments:
  output-dir    Directory to write results and metadata into

Environment:
  NAMESPACE     Kubernetes namespace (default: kafka)

Prerequisites:
  - kubectl configured with access to the cluster
  - mvn process-sources run to generate filtered JBang sources
  - A benchmark run completed in the benchmark pod
EOF
    exit 1
}

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

# List result files on the pod
RESULT_FILES=$(kubectl exec -n "$NAMESPACE" "$POD" -- find "$BENCHMARK_RESULTS_DIR" -maxdepth 1 -name '*.json' -printf '%f\n' 2>/dev/null) || true
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

# Generate run metadata
echo "Generating run metadata..."
jbang "$FILTERED" --generate-run-metadata "$OUTPUT_DIR"

echo "Done. Results collected in $OUTPUT_DIR/"
