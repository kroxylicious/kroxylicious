#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# simple script to build and run the operator
cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" || exit
cd .. || exit

. "scripts/common.sh"

if ! minikube status
then
  info "starting minikube"
  minikube start
fi

TMP_INSTALL_DIR="$(mktemp -d)"
trap 'rm -rf -- "$TMP_INSTALL_DIR"' EXIT


if [[ -z ${IMAGE_TAG:="dev-git-$(git rev-parse HEAD)"} ]]; then
  error "No value specified for IMAGE_TAG"
  exit 1
fi

OPERATOR_PULL_SPEC=$(buildPullSpec "operator")

cd kroxylicious-operator || exit
info "installing kafka (no-op if already installed)"
kubectl create namespace kafka --save-config --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -n kafka -f 'https://strimzi.io/install/latest?namespace=kafka'
kubectl wait -n kafka deployment/strimzi-cluster-operator --for=condition=Available=True --timeout=300s
kubectl apply -n kafka -f https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml
kubectl wait -n kafka kafka/my-cluster --for=condition=Ready --timeout=300s

info "deleting example"
set +e
kubectl delete -f target/packaged/examples/simple/ --ignore-not-found=true --timeout=30s --grace-period=1

info "deleting kroxylicious-operator installation"
kubectl delete -n kroxylicious-operator all --all --timeout=30s --grace-period=1
kubectl delete -f target/packaged/install/ --ignore-not-found=true --timeout=30s --grace-period=1
set -e

info "deleting all kroxylicious.io resources and crds"
for crd in $(kubectl get crds -oname | grep kroxylicious.io | awk -F / '{ print $2 }');
  do
  export crd
  info "deleting resources for crd: $crd"
  kubectl delete -A --all "$(kubectl get crd "${crd}" -o=jsonpath='{.spec.names.singular}')" --timeout=30s --grace-period=1
  info "deleting crd: ${crd}"
  kubectl delete crd "${crd}"
done

info "installing kroxylicious-operator"
kubectl apply -f target/packaged/install/
info "installing simple proxy"
kubectl apply -f target/packaged/examples/simple/

if kubectl wait -n my-proxy kafkaproxy/simple --for=condition=Ready=True --timeout=300s
then
  info "simple proxy should now be available in-cluster at my-cluster.my-proxy.svc.cluster.local:9292"
else
  error "something went wrong waiting for proxy to become ready!"
  exit 1
fi
