#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail

if [[ -z $QUAY_ORG ]]; then
  echo "Please set QUAY_ORG, exiting"
  exit 1
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/common.sh"
cd "${SCRIPT_DIR}/.."

if [[ "${QUAY_ORG}" != 'kroxylicious' ]]; then
  echo "building and pushing image to quay.io"
  PUSH_IMAGE=y "${SCRIPT_DIR}/deploy-image.sh"
else
  echo "QUAY_ORG is kroxylicious, not building/deploying image"
fi
set +e
MINIKUBE_MEM=$(${MINIKUBE} config get memory)
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
INSTALL_DIR="sample-install"
${MINIKUBE} start "${MINIKUBE_CONF}"
${KUBECTL} create namespace kafka || true
${KUBECTL} apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
${KUBECTL} apply -f "${INSTALL_DIR}/kafka-persistent.yaml" -n kafka
${KUBECTL} wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka 
${SED} -i "s/quay\.io\/kroxylicious/quay\.io\/${QUAY_ORG}/g" "${INSTALL_DIR}"/*
${KUBECTL} apply -f "${INSTALL_DIR}" -n kafka
${KUBECTL} wait deployment/kroxylicious-proxy --for=condition=Available=True --timeout=300s -n kafka 

echo "To produce to kroxylicious:"
echo "kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.0-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server kroxylicious-service:9292 --topic my-topic"
echo "To consume from kroxylicious:"
echo "kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.0-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server kroxylicious-service:9292 --topic my-topic --from-beginning"
