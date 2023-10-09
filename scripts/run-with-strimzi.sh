#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail
DEFAULT_REGISTRY_DESTINATION='quay.io/kroxylicious/kroxylicious-developer'
REGISTRY_DESTINATION=${REGISTRY_DESTINATION:-${DEFAULT_REGISTRY_DESTINATION}}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/common.sh"
cd "${SCRIPT_DIR}/.."

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <SAMPLE_DIR>"
  exit 1
elif [[ ! -d "${1}"  ]]; then
  echo "$0: Sample directory ${1} does not exist."
  exit 1
fi

SAMPLE_DIR=${1}

KUSTOMIZE_TMP=$(mktemp -d)
function cleanTmpDir {
  rm -rf "${KUSTOMIZE_TMP}"
}
trap cleanTmpDir EXIT

if [[ "${REGISTRY_DESTINATION}" != "${DEFAULT_REGISTRY_DESTINATION}" ]]; then
  echo "building and pushing image to ${REGISTRY_DESTINATION}"
  PUSH_IMAGE=y "${SCRIPT_DIR}/deploy-image.sh"
else
  echo "REGISTRY_DESTINATION is ${REGISTRY_DESTINATION}, not building/deploying image"
fi

set +e
MINIKUBE_MEM=$(${MINIKUBE} config get memory 2>/dev/null)
MINIKUBE_MEM_EXIT=$?
set -e
MINIKUBE_CONF='--memory=4096'
if [ $MINIKUBE_MEM_EXIT -eq 0 ]; then
  if [[ "$MINIKUBE_MEM" -lt 4096 ]]; then
    echo "minikube memory is configured to below 4096 by user, overriding to 4096"
  else
    echo "minikube memory configured by user"
    MINIKUBE_CONF=''
  fi
else
  echo "no minikube memory configuration, defaulting to 4096M"
fi
${MINIKUBE} start "${MINIKUBE_CONF}"

NAMESPACE=kafka

# Prepare kustomize overlay
cp -r "${SAMPLE_DIR}" "${KUSTOMIZE_TMP}"
OVERLAY_DIR=$(find "${KUSTOMIZE_TMP}" -type d -name minikube)

if [[ ! -d "${OVERLAY_DIR}" ]]; then
     echo "$0: Cannot find minikube overlay within sample."
     exit 1
fi

pushd "${OVERLAY_DIR}"
${KUSTOMIZE} edit set namespace ${NAMESPACE}
if [[ "${REGISTRY_DESTINATION}" != "${DEFAULT_REGISTRY_DESTINATION}" ]]; then
  ${KUSTOMIZE} edit set image "${DEFAULT_REGISTRY_DESTINATION}=${REGISTRY_DESTINATION}"
fi
popd

# Install certmanager
${KUBECTL} apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.12.0/cert-manager.yaml
${KUBECTL} wait deployment/cert-manager-webhook --for=condition=Available=True --timeout=300s -n cert-manager

# Install strimzi
${KUBECTL} create namespace kafka 2>/dev/null || true
${KUBECTL} apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
${KUBECTL} wait deployment strimzi-cluster-operator --for=condition=Available=True --timeout=300s -n kafka

# Apply sample using Kustomize
COUNTER=0
while ! ${KUBECTL} apply -k "${OVERLAY_DIR}"; do
  echo "Retrying ${KUBECTL} apply -k ${OVERLAY_DIR} .. probably a transient webhook issue."
  # Sometimes the certmgr's muting webhook is not ready, so retry
  (( COUNTER++ )) || true
  sleep 5
  if [[ "${COUNTER}" -gt 10 ]]; then
    echo "$0: Cannot apply sample."
    exit 1
  fi
done
echo "Config successfully applied."

${KUBECTL} wait kafka/my-cluster --for=condition=Ready --timeout=300s -n ${NAMESPACE}
${KUBECTL} wait deployment/kroxylicious-proxy --for=condition=Available=True --timeout=300s -n ${NAMESPACE}
echo "Deployments ready."

if [[ -f "${SAMPLE_DIR}"/postinstall.sh ]]; then
   "${SAMPLE_DIR}"/postinstall.sh
fi

