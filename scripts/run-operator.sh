#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

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


cd kroxylicious-operator || exit
info "installing kafka (no-op if already installed)"
kubectl create namespace kafka
kubectl create -n kafka -f 'https://strimzi.io/install/latest?namespace=kafka'
kubectl wait -n kafka deployment/strimzi-cluster-operator --for=condition=Available=True --timeout=300s
kubectl apply -n kafka -f https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml
kubectl wait -n kafka kafka/my-cluster --for=condition=Ready --timeout=300s

info "deleting example"
kubectl delete -f examples/simple/ --ignore-not-found=true --timeout=30s --grace-period=1

info "deleting kroxylicious-operator installation"
kubectl delete -n kroxylicious-operator all --all --timeout=30s --grace-period=1
kubectl delete -f install --ignore-not-found=true --timeout=30s --grace-period=1

info "deleting all kroxylicious.io resources and crds"
for crd in $(kubectl get crds -oname | grep kroxylicious.io | awk -F / '{ print $2 }');
  do
  export crd
  info "deleting resources for crd: $crd"
  kubectl delete -A --all "$(kubectl get crd "${crd}" -o=jsonpath='{.spec.names.singular}')" --timeout=30s --grace-period=1
  info "deleting crd: ${crd}"
  kubectl delete crd "${crd}"
done

info "installing crds"
kubectl apply -f ../kroxylicious-kubernetes-api/src/main/resources/META-INF/fabric8
info "installing kroxylicious-operator"
cp install/* ${TMP_INSTALL_DIR}
${SED} -i "s|quay.io/kroxylicious/operator:latest|quay.io/kroxylicious/operator:${IMAGE_TAG}|g" ${TMP_INSTALL_DIR}/03.Deployment.kroxylicious-operator.yaml
kubectl apply -f ${TMP_INSTALL_DIR}
info "installing simple proxy"
kubectl apply -f examples/simple/

if kubectl wait -n my-proxy kafkaproxy/simple --for=condition=Ready=True --timeout=300s
then
  info "simple proxy should now be available in-cluster at my-cluster.my-proxy.svc.cluster.local:9292"
else
  error "something went wrong waiting for proxy to become ready!"
  exit 1
fi
