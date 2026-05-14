#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# Polls the Kafka JMX Prometheus exporter during a benchmark run.
# Intended to be started as a background process by run-benchmark.sh.
#
# The exporter is only available when kafka.metrics.enabled=true in the Helm chart.
# If the endpoint does not respond within the initial timeout, this script exits
# cleanly (exit 0) so run-benchmark.sh is not disrupted when metrics are disabled.
#
# Each poll appends a snapshot header followed by the raw Prometheus text format output.
# The header line format is:
#   # SNAPSHOT timestamp=<unix_epoch_seconds> datetime=<ISO8601>
#
# Usage: poll-kafka-metrics.sh <broker-pod> <namespace> <output-dir> [interval-seconds]

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") <broker-pod> <namespace> <output-dir> [interval-seconds]

Polls the Kafka JMX Prometheus exporter endpoint (/metrics on port 9404) via
kubectl port-forward and appends timestamped Prometheus snapshots to
<output-dir>/kafka-metrics.txt.

Exits cleanly (exit 0) if the endpoint does not respond within 15 seconds —
this happens when kafka.metrics.enabled=false in the Helm chart.

Arguments:
  broker-pod       Kubernetes pod name for a Kafka broker
  namespace        Kubernetes namespace containing the pod
  output-dir       Directory to write kafka-metrics.txt into
  interval-seconds Polling interval in seconds (default: 30)
EOF
    exit 1
}

if [[ $# -lt 3 ]]; then
    usage
fi

BROKER_POD="$1"
NAMESPACE="$2"
OUTPUT_DIR="$3"
INTERVAL="${4:-30}"

METRICS_FILE="${OUTPUT_DIR}/kafka-metrics.txt"
LOCAL_PORT=19404

cleanup() {
    if [[ -n "${PF_PID:-}" ]]; then
        kill "${PF_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

mkdir -p "${OUTPUT_DIR}"

echo "Starting port-forward to ${BROKER_POD}:9404 on localhost:${LOCAL_PORT}..."
kubectl port-forward "pod/${BROKER_POD}" "${LOCAL_PORT}:9404" \
    -n "${NAMESPACE}" &>/dev/null &
PF_PID=$!

# Wait for endpoint to respond. Exit cleanly if it doesn't — JMX exporter is not deployed.
echo "Waiting for Kafka JMX metrics endpoint to be ready..."
PF_DEADLINE=$((SECONDS + 15))
until curl -sf "http://localhost:${LOCAL_PORT}/metrics" >/dev/null 2>&1; do
    if [[ $SECONDS -ge $PF_DEADLINE ]]; then
        echo "Kafka JMX metrics endpoint not available on ${BROKER_POD}:9404 — skipping Kafka metrics collection" \
            "(enable with kafka.metrics.enabled=true in cluster-overrides.yaml)" >&2
        exit 0
    fi
    if ! kill -0 "${PF_PID}" 2>/dev/null; then
        echo "Kafka metrics port-forward exited — JMX exporter likely not deployed" >&2
        exit 0
    fi
    sleep 1
done
echo "Kafka JMX metrics endpoint ready."

{
    echo "# kafka-metrics polling started"
    echo "# broker=${BROKER_POD} namespace=${NAMESPACE} interval=${INTERVAL}s"
    echo "# started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "${METRICS_FILE}"

while true; do
    NOW=$(date +%s)
    {
        echo ""
        echo "# SNAPSHOT datetime=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "# HELP benchmark_sample_timestamp_seconds Unix timestamp of this metrics snapshot"
        echo "# TYPE benchmark_sample_timestamp_seconds gauge"
        echo "benchmark_sample_timestamp_seconds ${NOW}"
        if ! curl -sf "http://localhost:${LOCAL_PORT}/metrics"; then
            echo "# WARNING: kafka metrics fetch failed at ${NOW}"
        fi
    } >> "${METRICS_FILE}"
    sleep "${INTERVAL}"
done
