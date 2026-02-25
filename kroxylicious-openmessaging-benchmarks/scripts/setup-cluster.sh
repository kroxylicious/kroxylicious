#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Operator versions (override via environment variables)
STRIMZI_VERSION="${STRIMZI_VERSION:-latest}"
KROXYLICIOUS_VERSION="${KROXYLICIOUS_VERSION:-0.18.0}"

NAMESPACE="${NAMESPACE:-kafka}"
KROXYLICIOUS_OPERATOR_NAMESPACE="kroxylicious-operator"

STRIMZI_INSTALL_URL="https://strimzi.io/install/${STRIMZI_VERSION}?namespace=${NAMESPACE}"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [--skip-kroxylicious]

Sets up a Kubernetes cluster with the operators required to run benchmarks.
Installs the Strimzi and (optionally) Kroxylicious operators and waits for
them to be ready.

Options:
  --skip-kroxylicious   Skip Kroxylicious operator installation (baseline scenario only)
  -h, --help            Show this help

Environment:
  NAMESPACE                     Kafka namespace (default: kafka)
  STRIMZI_VERSION               Strimzi version to install (default: latest)
  KROXYLICIOUS_VERSION          Kroxylicious operator version to install (default: 0.18.0)

Examples:
  # Full setup (baseline + proxy scenarios)
  $(basename "$0")

  # Baseline only (no Kroxylicious operator needed)
  $(basename "$0") --skip-kroxylicious

  # Pin operator versions
  STRIMZI_VERSION=0.45.0 KROXYLICIOUS_VERSION=0.18.0 $(basename "$0")
EOF
    exit 1
}

SKIP_KROXYLICIOUS=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-kroxylicious)
            SKIP_KROXYLICIOUS=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Error: unknown argument '$1'" >&2
            usage
            ;;
    esac
done

echo "=== Benchmark cluster setup ==="
echo "Namespace:            ${NAMESPACE}"
echo "Strimzi version:      ${STRIMZI_VERSION}"
if [[ "${SKIP_KROXYLICIOUS}" == "false" ]]; then
    echo "Kroxylicious version: ${KROXYLICIOUS_VERSION}"
fi
echo ""

# --- Namespace ---

if kubectl get namespace "${NAMESPACE}" &>/dev/null; then
    echo "Namespace '${NAMESPACE}' already exists, skipping creation"
else
    echo "Creating namespace '${NAMESPACE}'..."
    kubectl create namespace "${NAMESPACE}"
fi

# --- Strimzi operator ---

echo ""
echo "Installing Strimzi operator (${STRIMZI_VERSION})..."
kubectl create -f "${STRIMZI_INSTALL_URL}" -n "${NAMESPACE}" 2>/dev/null \
    || kubectl apply -f "${STRIMZI_INSTALL_URL}" -n "${NAMESPACE}"

echo "Waiting for Strimzi operator to be ready..."
kubectl wait --for=condition=ready pod \
    -l name=strimzi-cluster-operator \
    -n "${NAMESPACE}" \
    --timeout=300s

echo "Strimzi operator is ready."

# --- Kroxylicious operator ---

if [[ "${SKIP_KROXYLICIOUS}" == "true" ]]; then
    echo ""
    echo "Skipping Kroxylicious operator installation (--skip-kroxylicious)."
else
    echo ""
    echo "Installing Kroxylicious operator (${KROXYLICIOUS_VERSION})..."

    WORK_DIR="$(mktemp -d)"
    trap 'rm -rf "${WORK_DIR}"' EXIT

    TARBALL="kroxylicious-operator-${KROXYLICIOUS_VERSION}.tar.gz"
    echo "  Downloading ${TARBALL}..."
    gh release download "v${KROXYLICIOUS_VERSION}" \
        --repo kroxylicious/kroxylicious \
        --pattern "${TARBALL}" \
        --output "${WORK_DIR}/${TARBALL}"

    echo "  Extracting..."
    tar xzf "${WORK_DIR}/${TARBALL}" -C "${WORK_DIR}"

    echo "  Applying manifests..."
    kubectl apply -f "${WORK_DIR}/install/"

    echo "Waiting for Kroxylicious operator to be ready..."
    kubectl wait --for=condition=ready pod \
        -l app=kroxylicious \
        -n "${KROXYLICIOUS_OPERATOR_NAMESPACE}" \
        --timeout=300s

    echo "Kroxylicious operator is ready."
fi

echo ""
echo "=== Cluster setup complete ==="
echo ""
echo "Next steps:"
echo "  Run a benchmark:      ./scripts/run-benchmark.sh baseline 1topic-1kb ./results/"
echo "  Run all scenarios:    ./scripts/run-all-scenarios.sh ./results/"