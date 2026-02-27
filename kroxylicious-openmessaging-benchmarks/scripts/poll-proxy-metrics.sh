#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# Polls the proxy management endpoint during a benchmark run.
# Intended to be started as a background process by run-benchmark.sh.
#
# Each poll appends a snapshot header followed by the raw Prometheus text format output.
# The header line format is:
#   # SNAPSHOT timestamp=<unix_epoch_seconds> datetime=<ISO8601>
#
# Usage: poll-proxy-metrics.sh <proxy-pod> <namespace> <output-dir> [interval-seconds]

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") <proxy-pod> <namespace> <output-dir> [interval-seconds]

Polls the Kroxylicious proxy management endpoint (/metrics) via kubectl port-forward
and appends timestamped Prometheus snapshots to <output-dir>/proxy-metrics.txt.

Arguments:
  proxy-pod        Name of the proxy pod to port-forward to
  namespace        Kubernetes namespace containing the pod
  output-dir       Directory to write proxy-metrics.txt into
  interval-seconds Polling interval in seconds (default: 30)
EOF
    exit 1
}

if [[ $# -lt 3 ]]; then
    usage
fi

PROXY_POD="$1"
NAMESPACE="$2"
OUTPUT_DIR="$3"
INTERVAL="${4:-30}"

METRICS_FILE="${OUTPUT_DIR}/proxy-metrics.txt"
# Use a high local port to avoid collisions
LOCAL_PORT=19190

cleanup() {
    if [[ -n "${PF_PID:-}" ]]; then
        kill "${PF_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

mkdir -p "${OUTPUT_DIR}"

echo "Starting port-forward to ${PROXY_POD}:9190 on localhost:${LOCAL_PORT}..."
kubectl port-forward "pod/${PROXY_POD}" "${LOCAL_PORT}:9190" \
    -n "${NAMESPACE}" &>/dev/null &
PF_PID=$!

# Give port-forward a moment to establish
sleep 2

{
    echo "# proxy-metrics polling started"
    echo "# pod=${PROXY_POD} namespace=${NAMESPACE} interval=${INTERVAL}s"
    echo "# started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "${METRICS_FILE}"

while true; do
    {
        echo ""
        echo "# SNAPSHOT timestamp=$(date +%s) datetime=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        if ! curl -sf "http://localhost:${LOCAL_PORT}/metrics"; then
            echo "# WARNING: metrics fetch failed at $(date +%s)"
        fi
    } >> "${METRICS_FILE}"
    sleep "${INTERVAL}"
done
