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

NAMESPACE="${NAMESPACE:-kafka}"
HELM_RELEASE="benchmark"
KAFKA_READY_TIMEOUT="${KAFKA_READY_TIMEOUT:-600s}"
POD_READY_TIMEOUT="${POD_READY_TIMEOUT:-300s}"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") <scenario> <workload> <output-dir>

Runs a single benchmark scenario end-to-end:
  1. Deploy benchmark infrastructure via Helm
  2. Wait for Kafka and OMB pods to be ready
  3. Execute the OMB benchmark
  4. Collect results and metadata
  5. Tear down infrastructure (Helm uninstall + PVC cleanup)

Arguments:
  scenario    Scenario name matching a file in helm/scenarios/<scenario>-values.yaml
              Available: baseline, proxy-no-filters, smoke
  workload    Workload name (e.g. 1topic-1kb, 10topics-1kb, 100topics-1kb)
  output-dir  Directory to write result JSON and run metadata into

Options:
  -h, --help  Show this help

Environment:
  NAMESPACE              Kubernetes namespace (default: kafka)
  KAFKA_READY_TIMEOUT    Timeout waiting for Kafka to be ready (default: 600s)
  POD_READY_TIMEOUT      Timeout waiting for pods to be ready (default: 300s)

Examples:
  $(basename "$0") baseline 1topic-1kb ./results/baseline/
  $(basename "$0") proxy-no-filters 1topic-1kb ./results/proxy-no-filters/
  NAMESPACE=benchmarks $(basename "$0") baseline 1topic-1kb ./results/
EOF
    exit 1
}

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
    usage
fi

if [[ $# -ne 3 ]]; then
    echo "Error: expected 3 arguments, got $#" >&2
    usage
fi

SCENARIO="$1"
WORKLOAD="$2"
OUTPUT_DIR="$3"

SCENARIO_VALUES="${HELM_CHART}/scenarios/${SCENARIO}-values.yaml"

if [[ ! -f "${SCENARIO_VALUES}" ]]; then
    echo "Error: scenario values file not found: ${SCENARIO_VALUES}" >&2
    echo "Available scenarios:" >&2
    ls "${HELM_CHART}/scenarios/"*-values.yaml 2>/dev/null \
        | sed 's|.*/\(.*\)-values.yaml|\1|' | sed 's/^/  /' >&2
    exit 1
fi

teardown() {
    echo ""
    echo "--- Tearing down benchmark infrastructure ---"
    if helm status "${HELM_RELEASE}" -n "${NAMESPACE}" &>/dev/null; then
        helm uninstall "${HELM_RELEASE}" -n "${NAMESPACE}"
    fi
    # Delete Kafka PVCs to avoid cluster ID conflicts on next install
    kubectl delete pvc -l strimzi.io/cluster=kafka -n "${NAMESPACE}" --ignore-not-found
    echo "Teardown complete."
}

echo "=== Benchmark run: ${SCENARIO} / ${WORKLOAD} ==="
echo "Namespace:  ${NAMESPACE}"
echo "Output dir: ${OUTPUT_DIR}"
echo ""

# Ensure previous run is cleaned up before starting
if helm status "${HELM_RELEASE}" -n "${NAMESPACE}" &>/dev/null; then
    echo "Warning: Helm release '${HELM_RELEASE}' already exists. Tearing down before proceeding."
    teardown
fi

# --- Deploy ---

echo "--- Deploying benchmark infrastructure (${SCENARIO}) ---"
helm install "${HELM_RELEASE}" "${HELM_CHART}" \
    -n "${NAMESPACE}" \
    -f "${SCENARIO_VALUES}" \
    --set omb.workload="${WORKLOAD}"

# Register teardown to run on exit (success or failure) so we always clean up
trap teardown EXIT

# --- Wait for Kafka ---

echo "Waiting for Kafka cluster to be ready (timeout: ${KAFKA_READY_TIMEOUT})..."
kubectl wait kafka/kafka \
    --for=condition=Ready \
    --timeout="${KAFKA_READY_TIMEOUT}" \
    -n "${NAMESPACE}"
echo "Kafka is ready."

# --- Wait for OMB pods ---

echo "Waiting for OMB workers to be ready..."
kubectl rollout status statefulset/omb-worker -n "${NAMESPACE}" --timeout="${POD_READY_TIMEOUT}"

echo "Waiting for OMB benchmark pod to be ready..."
kubectl wait --for=condition=ready pod \
    -l app=omb-benchmark \
    -n "${NAMESPACE}" \
    --timeout="${POD_READY_TIMEOUT}"
echo "OMB pods are ready."

# --- Run benchmark ---

echo ""
echo "--- Running benchmark (${SCENARIO} / ${WORKLOAD}) ---"
kubectl exec deploy/omb-benchmark -n "${NAMESPACE}" -- \
    sh -c '/opt/benchmark/bin/benchmark --drivers /etc/omb/driver/driver-kafka.yaml --workers "$WORKERS" /etc/omb/workloads/workload.yaml'

# --- Collect results ---

echo ""
echo "--- Collecting results ---"
mkdir -p "${OUTPUT_DIR}"
"${SCRIPT_DIR}/collect-results.sh" "${OUTPUT_DIR}"

echo ""
echo "=== Benchmark complete: ${SCENARIO} / ${WORKLOAD} ==="
echo "Results written to: ${OUTPUT_DIR}"

# teardown runs via trap on EXIT