#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail
DEFAULT_KROXYLICIOUS_IMAGE='quay.io/kroxylicious/kroxylicious'
KROXYLICIOUS_IMAGE=${KROXYLICIOUS_IMAGE:-${DEFAULT_KROXYLICIOUS_IMAGE}}
STRIMZI_VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=strimzi.version -q -DforceStdout)

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

ON_SHUTDOWN=()
SAMPLE_DIR=${1}
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NOCOLOR='\033[0m'

onExit() {
  for cmd in "${ON_SHUTDOWN[@]}"
  do
    eval "${cmd}"
  done
}

trap onExit EXIT

KUSTOMIZE_TMP=$(mktemp -d)
function cleanTmpDir {
  rm -rf "${KUSTOMIZE_TMP}"
}
ON_SHUTDOWN+=(cleanTmpDir)

if ! ${MINIKUBE} status 1>/dev/null 2>/dev/null; then
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
  echo -e "${GREEN}minikube started.${NOCOLOR}"
else
  echo -e "${GREEN}minikube instance already available, we'll use it.${NOCOLOR}"
fi

# Prepare kustomize overlay
cp -r "${SAMPLE_DIR}" "${KUSTOMIZE_TMP}"
OVERLAY_DIR=$(find "${KUSTOMIZE_TMP}" -type d -name minikube)

if [[ ! -d "${OVERLAY_DIR}" ]]; then
     echo "$0: Cannot find minikube overlay within sample."
     exit 1
fi

pushd "${OVERLAY_DIR}" > /dev/null
if [[ "${KROXYLICIOUS_IMAGE}" != "${DEFAULT_KROXYLICIOUS_IMAGE}" ]]; then
  ${KUSTOMIZE} edit set image "${DEFAULT_KROXYLICIOUS_IMAGE}=${KROXYLICIOUS_IMAGE}"
fi
popd > /dev/null

# Install cert-manager (if necessary)
if grep --count --quiet --recursive cert-manager.io "${SAMPLE_DIR}"; then
  ${KUBECTL} apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.12.0/cert-manager.yaml
  ${KUBECTL} wait deployment/cert-manager-webhook --for=condition=Available=True --timeout=300s -n cert-manager
  echo -e "${GREEN}cert-manager installed.${NOCOLOR}"
fi

# Install HashiCorp Vault (if necessary)
if [[ -f ${SAMPLE_DIR}/helm-vault-values.yaml ]];
then
  ${KUBECTL} create ns vault 2>/dev/null || true
  ${HELM} repo add hashicorp https://helm.releases.hashicorp.com
  # use helm's idempotent install technique
  ${HELM} upgrade --install vault hashicorp/vault --namespace vault --values "${SAMPLE_DIR}/helm-vault-values.yaml" --wait
  ${KUBECTL} exec vault-0 -n vault -- vault secrets enable transit || true
  echo -e "${GREEN}HashiCorp Vault installed.${NOCOLOR}"
  pipename="${TMPDIR}/vault_url"
  mkfifo "${pipename}"
  ${MINIKUBE} service vault -n vault --url 2>/dev/null > "${pipename}" &
  MINIKUBE_VAULT_PORT_FORWARD_PID=$!
  VAULT_UI_URL=$(head -1 "${pipename}")
  rm "${pipename}"
  ON_SHUTDOWN+=("kill -3 ${MINIKUBE_VAULT_PORT_FORWARD_PID}")

  ROOT_TOKEN=$(yq .server.dev.devRootToken "${SAMPLE_DIR}/helm-vault-values.yaml")
  echo -e "${YELLOW}Vault UI available at: ${VAULT_UI_URL} (token '${ROOT_TOKEN}').${NOCOLOR}"
fi

# Install strimzi
${KUBECTL} create namespace kafka 2>/dev/null || true
# use helm's idempotent install technique
${HELM} upgrade --install strimzi-cluster-operator --namespace kafka --timeout 120s --version "${STRIMZI_VERSION}" oci://quay.io/strimzi-helm/strimzi-kafka-operator --set replicas=1 --wait
echo -e "${GREEN}Strimzi installed.${NOCOLOR}"


# Apply sample using Kustomize
COUNTER=0
while ! ${KUBECTL} apply -k "${OVERLAY_DIR}"; do
  echo "Retrying ${KUBECTL} apply -k ${OVERLAY_DIR} .. probably a transient webhook issue."
  # Sometimes the cert-manager's muting webhook is not ready, so retry
  (( COUNTER++ )) || true
  sleep 5
  if [[ "${COUNTER}" -gt 10 ]]; then
    echo "$0: Cannot apply sample."
    exit 1
  fi
done
echo -e "${GREEN}Kafka and Kroxylicious config successfully applied.${NOCOLOR}"
echo "Waiting for resources to be ready..."

${KUBECTL} wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
if grep --extended-regexp --count --quiet --recursive '(cluster-ca|client-ca)' "${SAMPLE_DIR}"; then
  # Copy CA secrets to Kroxylicious namespace (if necessary)
  ${KUBECTL} -n kafka get secrets -n kafka -l strimzi.io/component-type=certificate-authority -o json | jq '.items[] |= ( .metadata |= {name} )' | ${KUBECTL} apply -n kroxylicious -f -
  ${KUBECTL} rollout -n kroxylicious restart deployment kroxylicious-proxy
fi
${KUBECTL} wait deployment/kroxylicious-proxy --for=condition=Available=True --timeout=300s -n kroxylicious
echo -e "${GREEN}Kafka and Kroxylicious deployments are ready.${NOCOLOR}"

if [[ -f "${SAMPLE_DIR}"/postinstall.sh ]]; then
   "${SAMPLE_DIR}"/postinstall.sh
fi

echo  -e  "${GREEN}Example installed successfully, to continue refer to ${SAMPLE_DIR}/README.md.${NOCOLOR}"