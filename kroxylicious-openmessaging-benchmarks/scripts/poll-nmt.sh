#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# Polls JVM Native Memory Tracking (NMT) from the OMB benchmark coordinator during a run.
# Intended to be started as a background process by run-benchmark.sh.
#
# Each poll appends a snapshot header followed by the raw jcmd VM.native_memory summary output.
# The header line format is:
#   # SNAPSHOT datetime=<ISO8601>
#
# Usage: poll-nmt.sh <target> <namespace> <output-dir> [interval-seconds]

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") <target> <namespace> <output-dir> [interval-seconds]

Polls JVM native memory (NMT) from an OMB coordinator pod via jcmd and appends
timestamped snapshots to <output-dir>/omb-nmt.txt.

Waits up to 120s for the benchmark JVM to appear before giving up.
Exits cleanly when the JVM process is no longer visible to jcmd.

Arguments:
  target           Kubernetes resource reference for the coordinator pod
                   (e.g. job/omb-benchmark or deploy/omb-benchmark)
  namespace        Kubernetes namespace containing the pod
  output-dir       Directory to write omb-nmt.txt into
  interval-seconds Polling interval in seconds (default: 10)
EOF
    exit 1
}

if [[ $# -lt 3 ]]; then
    usage
fi

TARGET="$1"
NAMESPACE="$2"
OUTPUT_DIR="$3"
INTERVAL="${4:-10}"

NMT_FILE="${OUTPUT_DIR}/omb-nmt.txt"

mkdir -p "${OUTPUT_DIR}"

# Wait for the benchmark JVM to appear — the pod may take a few seconds to
# start the JVM, so jcmd will not see it immediately.
echo "NMT poller: waiting for OMB benchmark JVM to appear in ${TARGET}..."
JVM_PID=""
DEADLINE=$((SECONDS + 120))
while [[ -z "${JVM_PID}" ]]; do
    if [[ $SECONDS -ge $DEADLINE ]]; then
        echo "ERROR: timed out waiting for OMB benchmark JVM" >&2
        exit 1
    fi
    JVM_PID=$(kubectl exec "${TARGET}" -n "${NAMESPACE}" -- \
        sh -c 'JAVA_TOOL_OPTIONS="" jcmd 2>/dev/null | awk "/^[0-9]+[[:space:]]/ && !/jdk\.jcmd/{print \$1; exit}"' 2>/dev/null) || true
    [[ -z "${JVM_PID}" ]] && sleep 2
done

echo "NMT poller: found JVM PID ${JVM_PID} — polling every ${INTERVAL}s to ${NMT_FILE}"

{
    echo "# omb-nmt NMT polling started"
    echo "# target=${TARGET} namespace=${NAMESPACE} pid=${JVM_PID} interval=${INTERVAL}s"
    echo "# started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "${NMT_FILE}"

while true; do
    {
        echo ""
        echo "# SNAPSHOT datetime=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        if ! kubectl exec "${TARGET}" -n "${NAMESPACE}" -- \
                sh -c "JAVA_TOOL_OPTIONS='' jcmd ${JVM_PID} VM.native_memory summary" 2>/dev/null; then
            echo "# WARNING: NMT fetch failed (JVM may have exited)"
            break
        fi
    } >> "${NMT_FILE}"
    sleep "${INTERVAL}"
done
