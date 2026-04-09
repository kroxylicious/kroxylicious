#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# Installs the OpenShift Local Storage Operator (LSO) and creates a LocalVolume
# backed by the specified block devices on every worker node.
#
# Prerequisites:
#   - OpenShift cluster with worker nodes that have unformatted block devices
#   - oc CLI authenticated as cluster-admin
#
# The script is idempotent: re-running it on a cluster where LSO is already
# installed will skip the steps that are already complete.

LSO_NAMESPACE="openshift-local-storage"
LSO_CHANNEL="${LSO_CHANNEL:-stable}"
STORAGE_CLASS_NAME="${STORAGE_CLASS_NAME:-local-disks}"
OPERATOR_READY_TIMEOUT="${OPERATOR_READY_TIMEOUT:-300s}"
PV_WAIT_TIMEOUT="${PV_WAIT_TIMEOUT:-300}"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") --devices <device>[,<device>...]

Installs the OpenShift Local Storage Operator and provisions a StorageClass
backed by local block devices on every worker node. The named devices must
exist and be unformatted on all worker nodes.

Options:
  --devices <list>          Comma-separated block device paths (e.g. /dev/vdb,/dev/vdc)
  --no-default-storage-class  Do not mark the StorageClass as the cluster default
                            (use this when the cluster already has a default you want to keep)
  -h, --help                Show this help

Environment:
  LSO_CHANNEL              OLM channel for the operator (default: ${LSO_CHANNEL})
  STORAGE_CLASS_NAME       Name of the StorageClass to create (default: ${STORAGE_CLASS_NAME})
  OPERATOR_READY_TIMEOUT   Timeout for operator rollout (default: ${OPERATOR_READY_TIMEOUT})
  PV_WAIT_TIMEOUT          Seconds to wait for PVs to appear (default: ${PV_WAIT_TIMEOUT})

Examples:
  # Provision /dev/vdb and /dev/vdc on all worker nodes (sets local-disks as default StorageClass)
  $(basename "$0") --devices /dev/vdb,/dev/vdc

  # Provision without changing the cluster default StorageClass
  $(basename "$0") --devices /dev/vdb,/dev/vdc --no-default-storage-class

  # Use a custom storage class name
  STORAGE_CLASS_NAME=benchmark-local $(basename "$0") --devices /dev/vdb
EOF
    exit 1
}

DEVICES=""
SET_DEFAULT_STORAGE_CLASS=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --devices) DEVICES="$2"; shift 2 ;;
        --no-default-storage-class) SET_DEFAULT_STORAGE_CLASS=false; shift ;;
        -h|--help) usage ;;
        *) echo "Error: unknown argument '$1'" >&2; usage ;;
    esac
done

if [[ -z "${DEVICES}" ]]; then
    echo "Error: --devices is required" >&2
    usage
fi

# Split devices into an array for use in the LocalVolume CR
IFS=',' read -ra DEVICE_ARRAY <<< "${DEVICES}"

echo "=== Local storage setup ==="
echo "LSO namespace:   ${LSO_NAMESPACE}"
echo "LSO channel:     ${LSO_CHANNEL}"
echo "StorageClass:    ${STORAGE_CLASS_NAME}"
echo "Devices:         ${DEVICES}"
echo ""

# --- Namespace ---

if kubectl get namespace "${LSO_NAMESPACE}" &>/dev/null; then
    echo "Namespace '${LSO_NAMESPACE}' already exists, skipping."
else
    echo "Creating namespace '${LSO_NAMESPACE}'..."
    kubectl create namespace "${LSO_NAMESPACE}"
fi

# --- OperatorGroup ---

if kubectl get operatorgroup -n "${LSO_NAMESPACE}" &>/dev/null && \
   [[ -n "$(kubectl get operatorgroup -n "${LSO_NAMESPACE}" -o name 2>/dev/null)" ]]; then
    echo "OperatorGroup already exists in '${LSO_NAMESPACE}', skipping."
else
    echo "Creating OperatorGroup..."
    kubectl apply -f - <<EOF
apiVersion: operators.coreos.com/v1
kind: OperatorGroup
metadata:
  name: local-storage
  namespace: ${LSO_NAMESPACE}
spec:
  targetNamespaces:
    - ${LSO_NAMESPACE}
EOF
fi

# --- Subscription ---

if kubectl get subscription local-storage-operator -n "${LSO_NAMESPACE}" &>/dev/null; then
    echo "Subscription already exists, skipping."
else
    echo "Creating Subscription (channel: ${LSO_CHANNEL})..."
    kubectl apply -f - <<EOF
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: local-storage-operator
  namespace: ${LSO_NAMESPACE}
spec:
  channel: ${LSO_CHANNEL}
  name: local-storage-operator
  source: redhat-operators
  sourceNamespace: openshift-marketplace
EOF
fi

# --- Wait for operator to be ready ---

echo "Waiting for LSO operator deployment to appear..."
for i in $(seq 1 60); do
    if kubectl get deployment local-storage-operator -n "${LSO_NAMESPACE}" &>/dev/null; then
        break
    fi
    sleep 5
done

if ! kubectl get deployment local-storage-operator -n "${LSO_NAMESPACE}" &>/dev/null; then
    echo "Error: LSO deployment did not appear within 5 minutes" >&2
    exit 1
fi

echo "Waiting for LSO operator to be ready..."
kubectl rollout status deployment/local-storage-operator \
    -n "${LSO_NAMESPACE}" \
    --timeout="${OPERATOR_READY_TIMEOUT}"

echo "LSO operator is ready."

# --- Discover worker nodes ---

echo ""
echo "Discovering worker nodes..."
WORKER_NODES=()
while IFS= read -r node; do
    WORKER_NODES+=("${node}")
done < <(kubectl get nodes -l node-role.kubernetes.io/worker \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')

if [[ ${#WORKER_NODES[@]} -eq 0 ]]; then
    echo "Error: no worker nodes found (looked for label node-role.kubernetes.io/worker)" >&2
    exit 1
fi

echo "Found ${#WORKER_NODES[@]} worker node(s): ${WORKER_NODES[*]}"

# --- Build nodeDeviceSelectors for the LocalVolume CR ---
# Each worker gets an entry with the same device list.

NODE_DEVICE_SELECTORS=""
for node in "${WORKER_NODES[@]}"; do
    NODE_DEVICE_SELECTORS+="    - nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values:
                - ${node}
      devicePaths:"$'\n'
    for dev in "${DEVICE_ARRAY[@]}"; do
        NODE_DEVICE_SELECTORS+="        - ${dev}"$'\n'
    done
done

# --- LocalVolume CR ---

if kubectl get localvolume "${STORAGE_CLASS_NAME}" -n "${LSO_NAMESPACE}" &>/dev/null; then
    echo "LocalVolume '${STORAGE_CLASS_NAME}' already exists, skipping."
else
    echo "Creating LocalVolume CR..."
    kubectl apply -f - <<EOF
apiVersion: local.storage.openshift.io/v1
kind: LocalVolume
metadata:
  name: ${STORAGE_CLASS_NAME}
  namespace: ${LSO_NAMESPACE}
spec:
  nodeSelector:
    nodeSelectorTerms:
      - matchExpressions:
          - key: node-role.kubernetes.io/worker
            operator: Exists
  storageClassDevices:
    - storageClassName: ${STORAGE_CLASS_NAME}
      volumeMode: Filesystem
      fsType: xfs
      devicePaths:
$(printf '        - %s\n' "${DEVICE_ARRAY[@]}")
EOF
fi

# --- Wait for PVs to appear ---

echo ""
echo "Waiting for PVs to be provisioned (up to ${PV_WAIT_TIMEOUT}s)..."
EXPECTED_PV_COUNT=$(( ${#WORKER_NODES[@]} * ${#DEVICE_ARRAY[@]} ))
echo "Expecting ${EXPECTED_PV_COUNT} PV(s) (${#WORKER_NODES[@]} nodes × ${#DEVICE_ARRAY[@]} device(s) each)"

count_available_pvs() {
    # Field selectors on spec/status fields are not supported for PVs — filter in jsonpath instead
    kubectl get pv \
        -o jsonpath="{range .items[?(@.spec.storageClassName=='${STORAGE_CLASS_NAME}')]}{.status.phase}{'\n'}{end}" \
        2>/dev/null \
        | grep -c "^Available$" || true
}

ELAPSED=0
while [[ ${ELAPSED} -lt ${PV_WAIT_TIMEOUT} ]]; do
    AVAILABLE=$(count_available_pvs)
    if [[ "${AVAILABLE}" -ge "${EXPECTED_PV_COUNT}" ]]; then
        break
    fi
    echo "  ${AVAILABLE}/${EXPECTED_PV_COUNT} PVs available, waiting..."
    sleep 10
    ELAPSED=$(( ELAPSED + 10 ))
done

AVAILABLE=$(count_available_pvs)

if [[ "${AVAILABLE}" -lt "${EXPECTED_PV_COUNT}" ]]; then
    echo "Warning: only ${AVAILABLE}/${EXPECTED_PV_COUNT} PVs became available within ${PV_WAIT_TIMEOUT}s" >&2
    echo "         Check LSO pod logs: kubectl logs -n ${LSO_NAMESPACE} -l app=local-volume-diskmaker" >&2
else
    echo "All ${AVAILABLE} PV(s) are available."
fi

if [[ "${SET_DEFAULT_STORAGE_CLASS}" == "true" ]]; then
    echo ""
    echo "Marking '${STORAGE_CLASS_NAME}' as the default StorageClass..."
    kubectl patch storageclass "${STORAGE_CLASS_NAME}" \
        -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
    echo "Done."
fi

echo ""
echo "=== Local storage setup complete ==="
echo ""
echo "StorageClass '${STORAGE_CLASS_NAME}' is ready."
if [[ "${SET_DEFAULT_STORAGE_CLASS}" == "true" ]]; then
    echo "It is set as the cluster default — no extra flags needed when running benchmarks."
else
    echo "To use it in benchmarks, pass: --set kafka.storage.storageClass=${STORAGE_CLASS_NAME}"
fi
echo ""
echo "Next step: run setup-cluster.sh to install Kafka and Kroxylicious operators."
