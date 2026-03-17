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
METRICS_INTERVAL="${METRICS_INTERVAL:-30}"

PROXY_POD_LABEL="app.kubernetes.io/name=kroxylicious,app.kubernetes.io/component=proxy,app.kubernetes.io/instance=benchmark-proxy"

# JFR configuration
JFR_MAX_SIZE_MB="${JFR_MAX_SIZE_MB:-64}"
JFR_MAX_SIZE="${JFR_MAX_SIZE_MB}m"
JFR_PVC_SIZE_MI=$(( JFR_MAX_SIZE_MB * 110 / 100 ))
JFR_PVC_SIZE="${JFR_PVC_SIZE_MI}Mi"
JFR_PVC_NAME="${HELM_RELEASE}-jfr"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [--profile <values-file>] [--set <key=value> ...] <scenario> <workload> <output-dir>

Runs a single benchmark scenario end-to-end:
  1. Deploy benchmark infrastructure via Helm
  2. Wait for Kafka and OMB pods to be ready
  3. Execute the OMB benchmark
  4. Collect results, JFR recording, and CPU flamegraph
  5. Tear down infrastructure (Helm uninstall + PVC cleanup)

When a proxy pod is present, JFR and async-profiler CPU profiling are enabled automatically.
Results include benchmark.jfr and flamegraph.html written to output-dir.

Arguments:
  scenario    Scenario name matching a file in helm/scenarios/<scenario>-values.yaml
              Available: baseline, proxy-no-filters
  workload    Workload name (e.g. 1topic-1kb, 10topics-1kb, 100topics-1kb)
  output-dir  Directory to write result JSON and run metadata into

Options:
  --profile <values-file>   Additional Helm values file layered on top of the scenario
                            (e.g. helm/kroxylicious-benchmark/scenarios/single-node-values.yaml)
  --set <key=value>         Pass a Helm --set override (may be repeated)
  --skip-teardown             Leave infrastructure running after the benchmark (or on failure).
                            Useful for post-failure debugging (e.g. jcmd, kubectl exec).
                            The script will print the teardown commands to run manually.
  -h, --help                Show this help

Environment:
  NAMESPACE              Kubernetes namespace (default: kafka)
  KAFKA_READY_TIMEOUT    Timeout waiting for Kafka to be ready (default: 600s)
  POD_READY_TIMEOUT      Timeout waiting for pods to be ready (default: 300s)
  METRICS_INTERVAL       Proxy metrics polling interval in seconds (default: 30)
  JFR_MAX_SIZE_MB        Maximum size of the JFR recording in megabytes (default: 64)

Examples:
  $(basename "$0") baseline 1topic-1kb ./results/baseline/
  $(basename "$0") --profile ./helm/kroxylicious-benchmark/scenarios/single-node-values.yaml \
    baseline 1topic-1kb ./results/baseline/
  $(basename "$0") --set benchmark.testDurationMinutes=1 --set benchmark.warmupDurationMinutes=0 \
    baseline 1topic-1kb ./results/baseline/
  NAMESPACE=benchmarks $(basename "$0") baseline 1topic-1kb ./results/
EOF
    exit 1
}

PROFILE_VALUES=""
HELM_SET_ARGS=()
SKIP_TEARDOWN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)
            PROFILE_VALUES="$2"
            shift 2
            ;;
        --set)
            HELM_SET_ARGS+=("$2")
            shift 2
            ;;
        --skip-teardown)
            SKIP_TEARDOWN=true
            shift
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

if [[ -n "${PROFILE_VALUES}" && ! -f "${PROFILE_VALUES}" ]]; then
    echo "Error: profile values file not found: ${PROFILE_VALUES}" >&2
    exit 1
fi

METRICS_PID=""

teardown() {
    echo ""
    echo "--- Tearing down benchmark infrastructure ---"
    stop_metrics_poller
    if helm status "${HELM_RELEASE}" -n "${NAMESPACE}" &>/dev/null; then
        helm uninstall "${HELM_RELEASE}" -n "${NAMESPACE}" --wait --timeout 120s
    fi
    # The proxy Deployment is managed by the Kroxylicious operator, not Helm directly.
    # helm uninstall --wait covers Helm-managed resources (e.g. the KafkaProxy CR) but
    # the operator may not have fully terminated the proxy pods by the time Helm returns.
    # Wait explicitly so the pvc-protection finalizer on the JFR PVC is released.
    kubectl wait pod -l "${PROXY_POD_LABEL}" -n "${NAMESPACE}" \
        --for=delete --timeout=60s 2>/dev/null || true
    # Delete Kafka PVCs to avoid cluster ID conflicts on next install
    kubectl delete pvc -l strimzi.io/cluster=kafka -n "${NAMESPACE}" --ignore-not-found --timeout=60s
    # Delete JFR PVC if one was created
    kubectl delete pvc "${JFR_PVC_NAME}" -n "${NAMESPACE}" --ignore-not-found --timeout=60s
    echo "Teardown complete."
}

start_metrics_poller() {
    local proxy_pod
    proxy_pod=$(kubectl get pod -n "${NAMESPACE}" \
        -l "${PROXY_POD_LABEL}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true
    if [[ -z "${proxy_pod}" ]]; then
        return
    fi
    echo "Starting proxy metrics polling (every ${METRICS_INTERVAL}s) for pod ${proxy_pod}..."
    mkdir -p "${OUTPUT_DIR}"
    "${SCRIPT_DIR}/poll-proxy-metrics.sh" \
        "pod/${proxy_pod}" "${NAMESPACE}" "${OUTPUT_DIR}" "${METRICS_INTERVAL}" &
    METRICS_PID=$!
    echo "Metrics poller running (PID ${METRICS_PID})"
}

stop_metrics_poller() {
    if [[ -n "${METRICS_PID}" ]]; then
        echo "Stopping metrics poller (PID ${METRICS_PID})..."
        kill "${METRICS_PID}" 2>/dev/null || true
        wait "${METRICS_PID}" 2>/dev/null || true
        METRICS_PID=""
    fi
}

echo "=== Benchmark run: ${SCENARIO} / ${WORKLOAD} ==="
echo "Namespace:  ${NAMESPACE}"
echo "Output dir: ${OUTPUT_DIR}"
if [[ -n "${PROFILE_VALUES}" ]]; then
    echo "Profile:    ${PROFILE_VALUES}"
fi
if [[ ${#HELM_SET_ARGS[@]} -gt 0 ]]; then
    echo "Overrides:  ${HELM_SET_ARGS[*]}"
fi
echo ""

# Ensure previous run is cleaned up before starting
if helm status "${HELM_RELEASE}" -n "${NAMESPACE}" &>/dev/null; then
    echo "Warning: Helm release '${HELM_RELEASE}' already exists. Tearing down before proceeding."
    teardown
fi

# --- Deploy ---

echo "--- Deploying benchmark infrastructure (${SCENARIO}) ---"
HELM_ARGS=(-n "${NAMESPACE}" -f "${SCENARIO_VALUES}")
[[ -n "${PROFILE_VALUES}" ]] && HELM_ARGS+=(-f "${PROFILE_VALUES}")
HELM_ARGS+=(--set omb.workload="${WORKLOAD}")
for set_arg in "${HELM_SET_ARGS[@]+"${HELM_SET_ARGS[@]}"}"; do HELM_ARGS+=(--set "${set_arg}"); done

helm install "${HELM_RELEASE}" "${HELM_CHART}" "${HELM_ARGS[@]}" --timeout 300s

# Register teardown to run on exit (success or failure) so we always clean up.
# Skipped when --skip-teardown is set, leaving infrastructure up for debugging.
if [[ "${SKIP_TEARDOWN}" == "false" ]]; then
    trap teardown EXIT
fi

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

# Note: the benchmark starts automatically inside the Job pod (no kubectl exec needed).
# run-benchmark.sh waits for the .done marker file written when the benchmark exits.

# --- Start JFR and async-profiler recording on proxy pod (if present) ---

PROXY_POD_LABEL="app.kubernetes.io/name=kroxylicious,app.kubernetes.io/component=proxy,app.kubernetes.io/instance=benchmark-proxy"
PROXY_DEPLOYMENT=""
PROXY_POD=""

PROXY_POD=$(kubectl get pod -n "${NAMESPACE}" -l "${PROXY_POD_LABEL}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true
if [[ -n "${PROXY_POD}" ]]; then
    PROXY_DEPLOYMENT=$(kubectl get deployment -n "${NAMESPACE}" \
        -l "${PROXY_POD_LABEL}" \
        -o jsonpath='{.items[0].metadata.name}')

    # Ensure any stale JFR PVC from a previous run is fully gone before creating a new one.
    # It may be Terminating (teardown in progress) or simply orphaned (--skip-teardown was used).
    # kubectl delete --timeout waits for full removal in both cases.
    if kubectl get pvc "${JFR_PVC_NAME}" -n "${NAMESPACE}" &>/dev/null; then
        echo "Deleting stale JFR PVC ${JFR_PVC_NAME} and waiting for full removal..."
        kubectl delete pvc "${JFR_PVC_NAME}" -n "${NAMESPACE}" --ignore-not-found --timeout=60s
    fi

    echo "Creating JFR PVC ${JFR_PVC_NAME} (${JFR_PVC_SIZE})..."
    JFR_PVC_NAME="${JFR_PVC_NAME}" NAMESPACE="${NAMESPACE}" JFR_PVC_SIZE="${JFR_PVC_SIZE}" \
        envsubst '${JFR_PVC_NAME} ${NAMESPACE} ${JFR_PVC_SIZE}' \
        < "${HELM_CHART}/patches/proxy-jfr-pvc.yaml" \
        | kubectl apply -f -

    echo "Patching proxy deployment ${PROXY_DEPLOYMENT} to mount JFR PVC at /tmp and enable JFR + async-profiler..."
    PROXY_DEPLOYMENT="${PROXY_DEPLOYMENT}" NAMESPACE="${NAMESPACE}" \
        JFR_PVC_NAME="${JFR_PVC_NAME}" JFR_MAX_SIZE="${JFR_MAX_SIZE}" \
        envsubst '${PROXY_DEPLOYMENT} ${NAMESPACE} ${JFR_PVC_NAME} ${JFR_MAX_SIZE}' \
        < "${HELM_CHART}/patches/proxy-jfr-tmp.yaml" \
        | kubectl apply --server-side --field-manager=benchmark-jfr -f -

    # Dry-run the async-profiler patch first — it sets seccompProfile: Unconfined which is
    # forbidden on clusters with restricted SCCs (e.g. OpenShift). If the dry-run reports
    # a PodSecurity violation, skip the patch and fall back to JFR-only recording.
    async_profiler_patch=$(PROXY_DEPLOYMENT="${PROXY_DEPLOYMENT}" NAMESPACE="${NAMESPACE}" \
        envsubst '${PROXY_DEPLOYMENT} ${NAMESPACE}' \
        < "${HELM_CHART}/patches/proxy-async-profiler.yaml")
    dry_run_output=$(echo "${async_profiler_patch}" \
        | kubectl apply --server-side --field-manager=benchmark-async-profiler --dry-run=server -f - 2>&1) || true
    if echo "${dry_run_output}" | grep -q "would violate PodSecurity"; then
        echo "Warning: cluster security policy forbids Unconfined seccomp — skipping async-profiler flamegraph." >&2
        echo "         JFR recording will still be collected." >&2
    else
        echo "${async_profiler_patch}" \
            | kubectl apply --server-side --field-manager=benchmark-async-profiler -f -
    fi

    echo "Waiting for proxy deployment rollout after patch..."
    kubectl rollout status deployment/"${PROXY_DEPLOYMENT}" \
        -n "${NAMESPACE}" \
        --timeout="${POD_READY_TIMEOUT}"

    # Re-fetch pod name — it changed after the rollout
    PROXY_POD=$(kubectl get pod -n "${NAMESPACE}" -l "${PROXY_POD_LABEL}" \
        -o jsonpath='{.items[0].metadata.name}')
    echo "JFR recording active on proxy pod ${PROXY_POD} (maxsize=${JFR_MAX_SIZE})"

    # Give the JVM a moment to initialise then check if async-profiler got perf events.
    # If it fell back to itimer mode, native stack frames will be absent from the flamegraph.
    sleep 3
    if kubectl logs "${PROXY_POD}" -n "${NAMESPACE}" --tail=100 2>/dev/null \
            | grep -qi "itimer\|perf_event_open\|perf.*not\|could not open\|Permission denied"; then
        echo "Warning: async-profiler may be running in itimer mode — perf_event_open was denied." >&2
        echo "         Native stack frames will not appear in the flamegraph." >&2
        echo "         Check: kubectl logs ${PROXY_POD} -n ${NAMESPACE} | grep -i profil" >&2
    else
        echo "async-profiler CPU profiling active."
    fi
fi

# --- Run benchmark ---

start_metrics_poller

echo ""
echo "--- Running benchmark (${SCENARIO} / ${WORKLOAD}) ---"

# The benchmark runs as the Job pod's main process. Poll for the .done marker
# file that the pod writes when the benchmark exits, then read the exit code.
BENCHMARK_POD=$(kubectl get pod -n "${NAMESPACE}" -l app=omb-benchmark \
    -o jsonpath='{.items[0].metadata.name}')
echo "Benchmark pod: ${BENCHMARK_POD}"
echo "Streaming benchmark output..."
kubectl logs -f "${BENCHMARK_POD}" -n "${NAMESPACE}" &
LOGS_PID=$!

DONE_FILE="/var/lib/omb/results/.done"
until kubectl exec "${BENCHMARK_POD}" -n "${NAMESPACE}" -- \
        test -f "${DONE_FILE}" 2>/dev/null; do
    # Check whether the pod has failed (e.g. OOM-killed) — if so it will never
    # write .done and the loop would otherwise hang indefinitely.
    pod_phase=$(kubectl get pod "${BENCHMARK_POD}" -n "${NAMESPACE}" \
        -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
    if [[ "${pod_phase}" == "Failed" ]]; then
        kill "${LOGS_PID}" 2>/dev/null || true
        wait "${LOGS_PID}" 2>/dev/null || true
        echo "Benchmark pod failed (phase: ${pod_phase}) before writing results — likely OOM-killed" >&2
        exit 1
    fi
    sleep 5
done
kill "${LOGS_PID}" 2>/dev/null || true
wait "${LOGS_PID}" 2>/dev/null || true

BENCHMARK_EXIT=$(kubectl exec "${BENCHMARK_POD}" -n "${NAMESPACE}" -- \
    cat "${DONE_FILE}" 2>/dev/null || echo "1")
if [[ "${BENCHMARK_EXIT}" != "0" ]]; then
    echo "Benchmark failed with exit code ${BENCHMARK_EXIT}" >&2
fi

# --- Dump JFR recording and flamegraph ---

if [[ -n "${PROXY_DEPLOYMENT}" ]]; then
    echo ""
    echo "--- Dumping JFR recording and flamegraph ---"
    # Re-fetch pod name — the operator may have rolled out a new pod during the benchmark
    PROXY_POD=$(kubectl get pod -n "${NAMESPACE}" -l "${PROXY_POD_LABEL}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true
    if [[ -z "${PROXY_POD}" ]]; then
        echo "Warning: proxy pod not found — skipping JFR and flamegraph dump" >&2
    fi
    JVM_PID=$(kubectl exec -n "${NAMESPACE}" "${PROXY_POD}" -- \
        sh -c 'JAVA_TOOL_OPTIONS="" jcmd 2>/dev/null | awk "/^[0-9]+[[:space:]]/ && !/jdk\.jcmd/{print \$1; exit}"')
    if [[ -z "${JVM_PID}" ]]; then
        echo "Warning: could not find JVM PID — skipping JFR and flamegraph dump" >&2
    else
        kubectl exec -n "${NAMESPACE}" "${PROXY_POD}" -- \
            sh -c "JAVA_TOOL_OPTIONS='' jcmd ${JVM_PID} JFR.stop name=benchmark filename=/tmp/benchmark.jfr"

        # Stop async-profiler and write the flamegraph to /tmp/flamegraph.html.
        # kroxylicious-start.sh exports ASYNC_PROFILER_LIB so the agent path is cleanly
        # readable from /proc/<pid>/environ.  We re-attach via jcmd JVMTI.agent_load with
        # a stop command — no asprof binary required.
        AGENT_LIB=$(kubectl exec -n "${NAMESPACE}" "${PROXY_POD}" -- \
            sh -c "tr '\0' '\n' < /proc/${JVM_PID}/environ | grep '^ASYNC_PROFILER_LIB=' | cut -d= -f2-")
        if [[ -z "${AGENT_LIB}" ]]; then
            echo "Warning: ASYNC_PROFILER_LIB not set in JVM environment — skipping flamegraph" >&2
        elif kubectl exec -n "${NAMESPACE}" "${PROXY_POD}" -- \
                sh -c "JAVA_TOOL_OPTIONS='' jcmd ${JVM_PID} JVMTI.agent_load ${AGENT_LIB} \
                       'stop,file=/tmp/flamegraph.html,output=flamegraph,title=${SCENARIO}/${WORKLOAD} $(date -u +%Y-%m-%dT%H:%M:%SZ)'"; then
            echo "Flamegraph written to /tmp/flamegraph.html"
        else
            echo "Warning: async-profiler stop failed — flamegraph may be incomplete or absent" >&2
        fi
    fi
    echo "Dump complete."
fi

stop_metrics_poller

# --- Collect results ---

echo ""
echo "--- Collecting results ---"
mkdir -p "${OUTPUT_DIR}"
JFR_PVC_NAME="${JFR_PVC_NAME}" "${SCRIPT_DIR}/collect-results.sh" "${OUTPUT_DIR}"

# Rename JFR and flamegraph files to include scenario and workload for clarity
[[ -f "${OUTPUT_DIR}/benchmark.jfr" ]] && mv "${OUTPUT_DIR}/benchmark.jfr" "${OUTPUT_DIR}/${SCENARIO}-${WORKLOAD}-benchmark.jfr"
[[ -f "${OUTPUT_DIR}/flamegraph.html" ]] && mv "${OUTPUT_DIR}/flamegraph.html" "${OUTPUT_DIR}/${SCENARIO}-${WORKLOAD}-flamegraph.html"

echo ""
echo "=== Benchmark complete: ${SCENARIO} / ${WORKLOAD} ==="
echo "Results written to: ${OUTPUT_DIR}"

if [[ "${SKIP_TEARDOWN}" == "true" ]]; then
    echo ""
    echo "Infrastructure left running (--skip-teardown). To tear down manually:"
    echo "  helm uninstall ${HELM_RELEASE} -n ${NAMESPACE}"
    echo "  kubectl delete pvc -l strimzi.io/cluster=kafka -n ${NAMESPACE} --ignore-not-found"
    echo "  kubectl delete pvc ${JFR_PVC_NAME} -n ${NAMESPACE} --ignore-not-found"
fi
# teardown runs via trap on EXIT (unless --skip-teardown was set)
exit "${BENCHMARK_EXIT:-0}"
