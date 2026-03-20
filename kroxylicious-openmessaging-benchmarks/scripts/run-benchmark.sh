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
# Maximum time to wait for a single benchmark to complete (seconds).
# Should exceed testDurationMinutes + warmupDurationMinutes + setup overhead.
PROBE_TIMEOUT="${PROBE_TIMEOUT:-3600}"
RESULTS_PVC_NAME="${RESULTS_PVC_NAME:-omb-sweep-results}"

PROXY_POD_LABEL="app.kubernetes.io/name=kroxylicious,app.kubernetes.io/component=proxy,app.kubernetes.io/instance=benchmark-proxy"

# JFR configuration
JFR_MAX_SIZE_MB="${JFR_MAX_SIZE_MB:-64}"
JFR_MAX_SIZE="${JFR_MAX_SIZE_MB}m"
JFR_PVC_SIZE_MI=$(( JFR_MAX_SIZE_MB * 110 / 100 ))
JFR_PVC_SIZE="${JFR_PVC_SIZE_MI}Mi"
JFR_PVC_NAME="${HELM_RELEASE}-jfr"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [--profile <values-file>] [--set <key=value> ...] [--skip-deploy] [--skip-teardown] [--producer-rate <n>] <scenario> <workload> <output-dir>

Runs a single benchmark scenario end-to-end:
  1. Deploy benchmark infrastructure via Helm  (skipped with --skip-deploy)
  2. Wait for Kafka and OMB pods to be ready   (skipped with --skip-deploy)
  3. Execute the OMB benchmark via kubectl exec
  4. Collect results, JFR recording, and CPU flamegraph  (JFR skipped with --skip-deploy)
  5. Tear down infrastructure (Helm uninstall + PVC cleanup)  (skipped with --skip-teardown)

When a proxy pod is present (and --skip-deploy is not set), JFR and async-profiler CPU profiling
are enabled automatically. Results include benchmark.jfr and flamegraph.html written to output-dir.

Arguments:
  scenario    Scenario name matching a file in helm/scenarios/<scenario>-values.yaml
              Available: baseline, proxy-no-filters
  workload    Workload name (e.g. 1topic-1kb, 10topics-1kb, 100topics-1kb)
  output-dir  Directory to write result JSON and run metadata into

Options:
  --profile <values-file>   Additional Helm values file layered on top of the scenario
                            (e.g. helm/kroxylicious-benchmark/scenarios/single-node-values.yaml)
  --set <key=value>         Pass a Helm --set override (may be repeated)
  --skip-deploy             Skip Helm deploy and pod wait; assume infrastructure is already up.
                            Resets Kafka topics before running. Used by rate-sweep.sh for probes
                            after the first. Implies no JFR setup or collection.
  --skip-teardown           Leave infrastructure running after the benchmark (or on failure).
                            Useful for post-failure debugging or multi-probe sweeps.
                            The script will print the teardown commands to run manually.
  --producer-rate <n>       Override the producerRate in the workload (msg/sec).
                            When set, the rate is injected via sed before running.
  -h, --help                Show this help

Environment:
  NAMESPACE                    Kubernetes namespace (default: kafka)
  KAFKA_READY_TIMEOUT          Timeout waiting for Kafka to be ready (default: 600s)
  POD_READY_TIMEOUT            Timeout waiting for pods to be ready (default: 300s)
  PROBE_TIMEOUT                Max seconds to wait for the benchmark to complete (default: 3600)
  METRICS_INTERVAL             Proxy metrics polling interval in seconds (default: 30)
  JFR_MAX_SIZE_MB              Maximum size of the JFR recording in megabytes (default: 64)

Examples:
  $(basename "$0") baseline 1topic-1kb ./results/baseline/
  $(basename "$0") --producer-rate 30000 baseline 1topic-1kb ./results/baseline/rate-30000/
  $(basename "$0") --skip-deploy --skip-teardown --producer-rate 50000 baseline 1topic-1kb ./results/baseline/rate-50000/
  $(basename "$0") --profile ./helm/kroxylicious-benchmark/scenarios/single-node-values.yaml \
    baseline 1topic-1kb ./results/baseline/
  NAMESPACE=benchmarks $(basename "$0") baseline 1topic-1kb ./results/
EOF
    exit 1
}

PROFILE_VALUES=""
HELM_SET_ARGS=()
SKIP_DEPLOY=false
SKIP_TEARDOWN=false
PRODUCER_RATE=""

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
        --skip-deploy)
            SKIP_DEPLOY=true
            shift
            ;;
        --skip-teardown)
            SKIP_TEARDOWN=true
            shift
            ;;
        --producer-rate)
            PRODUCER_RATE="$2"
            shift 2
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
LOGS_PID=""

teardown() {
    echo ""
    echo "--- Tearing down benchmark infrastructure ---"
    stop_logs_tailer
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
    # Delete any stale jfr-collect pod — it mounts the JFR PVC and prevents its deletion
    kubectl delete pod -l app=jfr-collect -n "${NAMESPACE}" --ignore-not-found --wait --timeout=60s
    # Delete Kafka PVCs to avoid cluster ID conflicts on next install
    kubectl delete pvc -l strimzi.io/cluster=kafka -n "${NAMESPACE}" --ignore-not-found --timeout=60s
    # Delete JFR PVC if one was created
    kubectl delete pvc "${JFR_PVC_NAME}" -n "${NAMESPACE}" --ignore-not-found --timeout=60s
    # Delete the benchmark Job (not Helm-managed so helm uninstall won't remove it)
    kubectl delete job omb-benchmark -n "${NAMESPACE}" --ignore-not-found --timeout=60s
    echo "Teardown complete."
}

check_workers_healthy() {
    local desired
    desired=$(kubectl get statefulset omb-worker -n "${NAMESPACE}" \
        -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
    if [[ "${desired}" == "0" ]]; then
        echo "OMB worker StatefulSet not found or has 0 replicas" >&2
        return 1
    fi

    # Wait for Kubernetes to consider all pods ready (TCP readiness probe)
    local ready
    ready=$(kubectl get statefulset omb-worker -n "${NAMESPACE}" \
        -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
    if [[ "${ready}" != "${desired}" ]]; then
        echo "OMB workers not fully ready (${ready}/${desired}) — waiting up to ${POD_READY_TIMEOUT}..."
        if ! kubectl rollout status statefulset/omb-worker \
                -n "${NAMESPACE}" --timeout="${POD_READY_TIMEOUT}" 2>/dev/null; then
            echo "OMB workers did not recover within ${POD_READY_TIMEOUT}" >&2
            return 1
        fi
    fi

}

reset_topics() {
    echo "Resetting Kafka topics..."
    local kafka_pod
    kafka_pod=$(kubectl get pod -n "${NAMESPACE}" \
        -l "strimzi.io/cluster=kafka,strimzi.io/pool-name=kafka-pool" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true
    if [[ -z "${kafka_pod}" ]]; then
        echo "Warning: no Kafka broker pod found — skipping topic reset" >&2
        return
    fi
    kubectl exec "${kafka_pod}" -n "${NAMESPACE}" -- \
        bash -c 'user_topics=$(/opt/kafka/bin/kafka-topics.sh \
                     --bootstrap-server localhost:9092 --list 2>/dev/null \
                     | grep -v "^__" || true)
                 if [[ -n "$user_topics" ]]; then
                     echo "$user_topics" | while IFS= read -r topic; do
                         /opt/kafka/bin/kafka-topics.sh \
                             --bootstrap-server localhost:9092 \
                             --delete --topic "$topic" 2>/dev/null || true
                     done
                     echo "Deleted topics: $(echo "$user_topics" | tr "\n" " ")"
                 else
                     echo "No user topics to delete."
                 fi'
    echo "Topic reset complete."
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

# Creates the results PVC if it does not already exist.
# The PVC is not managed by Helm — it persists across probes and Helm installs.
ensure_results_pvc() {
    if kubectl get pvc "${RESULTS_PVC_NAME}" -n "${NAMESPACE}" &>/dev/null; then
        return
    fi
    echo "Creating results PVC ${RESULTS_PVC_NAME}..."
    kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${RESULTS_PVC_NAME}
  namespace: ${NAMESPACE}
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 2Gi
EOF
}

# Deletes any existing benchmark Job then creates a fresh one using helm template,
# so the Job inherits all chart config (image, resources, ConfigMap names, etc.).
# HELM_ARGS must be set before calling this function.
create_benchmark_job() {
    echo "Creating benchmark Job (rate=${PRODUCER_RATE:-workload default})..."
    kubectl delete job omb-benchmark -n "${NAMESPACE}" --ignore-not-found --wait --timeout=60s
    helm template "${HELM_RELEASE}" "${HELM_CHART}" "${HELM_ARGS[@]}" \
        --set omb.createBenchmarkJob=true \
        --set "omb.coordinatorProducerRate=${PRODUCER_RATE:-}" \
        --show-only templates/omb-benchmark-job.yaml \
        | kubectl apply -n "${NAMESPACE}" -f -
}

# Spins up a temporary pod to mount the results PVC and copy result.json to OUTPUT_DIR.
# Used because exec into a completed/failed Job pod is not possible.
collect_result_from_pvc() {
    local reader_pod="omb-results-reader-$$"
    echo "Collecting result JSON from PVC ${RESULTS_PVC_NAME}..."
    kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${reader_pod}
  namespace: ${NAMESPACE}
  labels:
    app: omb-results-reader
spec:
  restartPolicy: Never
  volumes:
  - name: results
    persistentVolumeClaim:
      claimName: ${RESULTS_PVC_NAME}
  containers:
  - name: reader
    image: busybox:1.37.0@sha256:b3255e7dfbcd10cb367af0d409747d511aeb66dfac98cf30e97e87e4207dd76f
    command: ["sleep", "infinity"]
    volumeMounts:
    - name: results
      mountPath: /results
      readOnly: true
EOF
    local collected=false
    if kubectl wait pod "${reader_pod}" -n "${NAMESPACE}" \
            --for=condition=ready --timeout=120s 2>/dev/null; then
        mkdir -p "${OUTPUT_DIR}"
        if kubectl cp "${NAMESPACE}/${reader_pod}:/results/result.json" "${OUTPUT_DIR}/result.json" 2>/dev/null; then
            echo "  result.json"
            collected=true
        fi
    fi
    kubectl delete pod "${reader_pod}" -n "${NAMESPACE}" --ignore-not-found --wait=false 2>/dev/null || true
    if [[ "${collected}" != "true" ]]; then
        echo "Error: failed to collect result JSON from PVC ${RESULTS_PVC_NAME}" >&2
        return 1
    fi
}

echo "=== Benchmark run: ${SCENARIO} / ${WORKLOAD} ==="
echo "Namespace:  ${NAMESPACE}"
echo "Output dir: ${OUTPUT_DIR}"
if [[ -n "${PRODUCER_RATE}" ]]; then
    echo "Rate:       ${PRODUCER_RATE} msg/sec"
fi
if [[ -n "${PROFILE_VALUES}" ]]; then
    echo "Profile:    ${PROFILE_VALUES}"
fi
if [[ ${#HELM_SET_ARGS[@]} -gt 0 ]]; then
    echo "Overrides:  ${HELM_SET_ARGS[*]}"
fi
echo ""

# HELM_ARGS is used for both helm install (first probe) and helm template (all probes
# via create_benchmark_job), so it is built once here outside the deploy conditional.
HELM_ARGS=(-n "${NAMESPACE}" -f "${SCENARIO_VALUES}")
[[ -n "${PROFILE_VALUES}" ]] && HELM_ARGS+=(-f "${PROFILE_VALUES}")
HELM_ARGS+=(--set omb.workload="${WORKLOAD}")
for set_arg in "${HELM_SET_ARGS[@]+"${HELM_SET_ARGS[@]}"}"; do HELM_ARGS+=(--set "${set_arg}"); done

if [[ "${SKIP_DEPLOY}" == "false" ]]; then
    # Ensure previous run is cleaned up before starting
    if helm status "${HELM_RELEASE}" -n "${NAMESPACE}" &>/dev/null; then
        echo "Warning: Helm release '${HELM_RELEASE}' already exists. Tearing down before proceeding."
        teardown
    fi

    # --- Deploy ---

    echo "--- Deploying benchmark infrastructure (${SCENARIO}) ---"
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

    # --- Wait for OMB workers ---

    echo "Waiting for OMB workers to be ready..."
    kubectl rollout status statefulset/omb-worker -n "${NAMESPACE}" --timeout="${POD_READY_TIMEOUT}"
    echo "OMB workers are ready."

    ensure_results_pvc
else
    # Infrastructure already up — delete the previous benchmark Job, restart workers for
    # a clean probe, then reset topics.  Workers do not recover after a benchmark run ends
    # (the HTTP handler stops while the JVM stays alive), so we delete the pods and let the
    # StatefulSet recreate them fresh.
    echo "--- Skipping deploy (--skip-deploy) ---"
    echo "Removing previous benchmark Job..."
    kubectl delete job omb-benchmark -n "${NAMESPACE}" --ignore-not-found --wait --timeout=60s
    echo "Restarting OMB workers for clean probe..."
    kubectl delete pod -l app=omb-worker -n "${NAMESPACE}" --wait --timeout=60s
    check_workers_healthy
    reset_topics
fi

# --- Start JFR and async-profiler recording on proxy pod (if present, first run only) ---

PROXY_POD_LABEL="app.kubernetes.io/name=kroxylicious,app.kubernetes.io/component=proxy,app.kubernetes.io/instance=benchmark-proxy"
PROXY_DEPLOYMENT=""
PROXY_POD=""

PROXY_POD=$(kubectl get pod -n "${NAMESPACE}" -l "${PROXY_POD_LABEL}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true
if [[ -n "${PROXY_POD}" && "${SKIP_DEPLOY}" == "false" ]]; then
    PROXY_DEPLOYMENT=$(kubectl get deployment -n "${NAMESPACE}" \
        -l "${PROXY_POD_LABEL}" \
        -o jsonpath='{.items[0].metadata.name}')

    # Ensure any stale JFR PVC from a previous run is fully gone before creating a new one.
    # The pvc-protection finalizer is held while any pod mounts the volume, so if an old
    # proxy pod from a previous run is still terminating, the PVC cannot be deleted until
    # that pod fully exits.  We issue the delete and then poll rather than relying on a
    # fixed --timeout (which exits with an error if the deadline is hit).
    if kubectl get pvc "${JFR_PVC_NAME}" -n "${NAMESPACE}" &>/dev/null; then
        echo "Deleting stale JFR PVC ${JFR_PVC_NAME} (waiting for any terminating proxy pod)..."
        kubectl delete pvc "${JFR_PVC_NAME}" -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
        PVC_DEADLINE=$((SECONDS + 120))
        while kubectl get pvc "${JFR_PVC_NAME}" -n "${NAMESPACE}" &>/dev/null; do
            if [[ $SECONDS -ge $PVC_DEADLINE ]]; then
                echo "Warning: stale JFR PVC ${JFR_PVC_NAME} still present after 120s — proceeding anyway" >&2
                break
            fi
            sleep 5
        done
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

# Create the benchmark Job now — after JFR is set up on the proxy so profiling starts
# before load begins.  The Job is the entrypoint for the benchmark binary; it writes
# result JSON to the results PVC on exit.
create_benchmark_job

start_metrics_poller

echo ""
echo "--- Running benchmark (${SCENARIO} / ${WORKLOAD}) ---"
mkdir -p "${OUTPUT_DIR}"
echo "  OMB log: ${OUTPUT_DIR}/omb.log"

# Stream Job logs to file in the background (best-effort; survives brief disconnects).
# The benchmark process runs as the Job entrypoint and is NOT killed if the stream drops.
kubectl logs -f -n "${NAMESPACE}" -l app=omb-benchmark \
    >> "${OUTPUT_DIR}/omb.log" 2>/dev/null &
LOGS_PID=$!

# Poll for Job completion (Complete) or failure (Failed) every 10s.
# kubectl wait --for=condition=complete alone would block indefinitely on failure.
BENCHMARK_EXIT=0
PROBE_START=${SECONDS}
while true; do
    if kubectl wait job/omb-benchmark -n "${NAMESPACE}" \
            --for=condition=complete --timeout=10s 2>/dev/null; then
        break
    fi
    job_failed=$(kubectl get job omb-benchmark -n "${NAMESPACE}" \
        -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null) || true
    if [[ "${job_failed}" == "True" ]]; then
        BENCHMARK_EXIT=1
        break
    fi
    if [[ $((SECONDS - PROBE_START)) -ge ${PROBE_TIMEOUT} ]]; then
        BENCHMARK_EXIT=124
        break
    fi
done

kill "${LOGS_PID}" 2>/dev/null || true
wait "${LOGS_PID}" 2>/dev/null || true

if [[ "${BENCHMARK_EXIT}" -eq 124 ]]; then
    echo "TIMED OUT after ${PROBE_TIMEOUT}s" >&2
    echo "Last lines of OMB log:" >&2
    tail -10 "${OUTPUT_DIR}/omb.log" >&2 || true
elif [[ "${BENCHMARK_EXIT}" -ne 0 ]]; then
    echo "Benchmark failed (exit ${BENCHMARK_EXIT})" >&2
    echo "Last lines of OMB log:" >&2
    tail -10 "${OUTPUT_DIR}/omb.log" >&2 || true
fi


# --- Dump JFR recording and flamegraph ---

# Re-fetch proxy pod — the operator may have rolled a new pod during the benchmark.
# Warn if multiple pods are present: we only profile items[0] (see issue #3497).
PROXY_POD=$(kubectl get pod -n "${NAMESPACE}" -l "${PROXY_POD_LABEL}" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true
PROXY_POD_COUNT=$(kubectl get pod -n "${NAMESPACE}" -l "${PROXY_POD_LABEL}" \
    --no-headers 2>/dev/null | wc -l | tr -d ' ') || true
if [[ "${PROXY_POD_COUNT:-0}" -gt 1 ]]; then
    echo "Warning: ${PROXY_POD_COUNT} proxy pods found — profiling only ${PROXY_POD}." >&2
    echo "         Multi-pod profiling is not yet supported: https://github.com/kroxylicious/kroxylicious/issues/3497" >&2
fi

if [[ -n "${PROXY_POD}" ]]; then
    echo ""
    echo "--- Dumping JFR recording and flamegraph ---"
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

        # If more probes follow, restart a fresh recording for the next rate.
        if [[ "${SKIP_TEARDOWN}" == "true" ]]; then
            kubectl exec -n "${NAMESPACE}" "${PROXY_POD}" -- \
                sh -c "JAVA_TOOL_OPTIONS='' jcmd ${JVM_PID} JFR.start name=benchmark settings=default maxsize=${JFR_MAX_SIZE}"
            echo "JFR recording restarted for next probe."
        fi
    fi
    echo "Dump complete."
fi

stop_metrics_poller

# --- Collect results ---

echo ""
echo "--- Collecting results ---"
mkdir -p "${OUTPUT_DIR}"

# Result JSON always comes from the results PVC — the benchmark Job writes it there
# before exiting, and exec into a completed/failed pod is not possible.
collect_result_from_pvc

if [[ "${SKIP_DEPLOY}" == "false" ]]; then
    # Full collection: JFR dump + flamegraph + run metadata (JSON already collected above)
    JFR_PVC_NAME="${JFR_PVC_NAME}" PROXY_POD="${PROXY_POD:-}" \
        "${SCRIPT_DIR}/collect-results.sh" \
        --scenario "${SCENARIO}" --workload "${WORKLOAD}" --target-rate "${PRODUCER_RATE:-0}" \
        "${OUTPUT_DIR}"
    [[ -f "${OUTPUT_DIR}/benchmark.jfr" ]] && mv "${OUTPUT_DIR}/benchmark.jfr" "${OUTPUT_DIR}/${SCENARIO}-${WORKLOAD}-benchmark.jfr"
    [[ -f "${OUTPUT_DIR}/flamegraph.html" ]] && mv "${OUTPUT_DIR}/flamegraph.html" "${OUTPUT_DIR}/${SCENARIO}-${WORKLOAD}-flamegraph.html"
else
    # Lightweight collection: JFR/flamegraph only (JSON already collected above)
    if [[ -n "${PROXY_POD}" ]]; then
        local_jfr="${OUTPUT_DIR}/${SCENARIO}-${WORKLOAD}-benchmark.jfr"
        local_fg="${OUTPUT_DIR}/${SCENARIO}-${WORKLOAD}-flamegraph.html"
        kubectl exec -n "${NAMESPACE}" "${PROXY_POD}" -- cat /tmp/benchmark.jfr \
            > "${local_jfr}" 2>/dev/null || true
        if [[ -s "${local_jfr}" ]]; then
            echo "  ${SCENARIO}-${WORKLOAD}-benchmark.jfr ($(du -h "${local_jfr}" | cut -f1))"
        else
            echo "Warning: benchmark.jfr is empty — JFR dump may not have completed" >&2
            rm -f "${local_jfr}"
        fi
        if kubectl exec -n "${NAMESPACE}" "${PROXY_POD}" -- test -s /tmp/flamegraph.html 2>/dev/null; then
            kubectl exec -n "${NAMESPACE}" "${PROXY_POD}" -- cat /tmp/flamegraph.html \
                > "${local_fg}" 2>/dev/null || true
            echo "  ${SCENARIO}-${WORKLOAD}-flamegraph.html ($(du -h "${local_fg}" | cut -f1))"
        fi
    fi
fi

echo ""
echo "=== Benchmark complete: ${SCENARIO} / ${WORKLOAD} ==="
echo "Results written to: ${OUTPUT_DIR}"

if [[ "${SKIP_TEARDOWN}" == "true" ]]; then
    echo ""
    echo "Infrastructure left running (--skip-teardown). To tear down manually:"
    echo "  helm uninstall ${HELM_RELEASE} -n ${NAMESPACE} --wait"
    echo "  kubectl delete pod -l app=jfr-collect -n ${NAMESPACE} --ignore-not-found"
    echo "  kubectl delete pvc -l strimzi.io/cluster=kafka -n ${NAMESPACE} --ignore-not-found"
    echo "  kubectl delete pvc ${JFR_PVC_NAME} -n ${NAMESPACE} --ignore-not-found"
fi
# teardown runs via trap on EXIT (unless --skip-teardown was set)
exit "${BENCHMARK_EXIT}"
