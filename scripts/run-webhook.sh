#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/common.sh"
cd "${SCRIPT_DIR}/.."

info "Deleting existing minikube cluster"
${MINIKUBE} delete || true

info "Starting minikube with 4GB memory"
${MINIKUBE} start --memory=4096

info "Building distribution with Maven"
mvn clean install -Pdist -Dquick -pl :kroxylicious-admission-dist -am

info "Extracting distribution tarball"
DIST_DIR=$(mktemp -d)
tar -xzf kroxylicious-kubernetes/kroxylicious-admission-dist/target/kroxylicious-admission-*.tar.gz -C "${DIST_DIR}"

info "Loading webhook image into minikube"
gunzip --to-stdout kroxylicious-kubernetes/kroxylicious-admission/target/kroxylicious-webhook.img.tar.gz \
  | ${MINIKUBE} image load - --alsologtostderr=true 2>&1 | tee /dev/tty | tail -n1 | grep -q 'succeeded pushing to: minikube'

info "Loading proxy image into minikube"
gunzip --to-stdout kroxylicious-app/target/kroxylicious-proxy.img.tar.gz \
  | ${MINIKUBE} image load - --alsologtostderr=true 2>&1 | tee /dev/tty | tail -n1 | grep -q 'succeeded pushing to: minikube'

info "Installing cert-manager"
${KUBECTL} apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.20.2/cert-manager.yaml
${KUBECTL} wait deployment/cert-manager-webhook --for=condition=Available=True --timeout=300s -n cert-manager

info "Applying webhook installation manifests"
${KUBECTL} apply -f "${DIST_DIR}/install/"
${KUBECTL} apply -f "${DIST_DIR}/examples/cert-manager/"
${KUBECTL} wait deployment/kroxylicious-webhook --for=condition=Available=True --timeout=300s -n kroxylicious-webhook

info "Installing Strimzi operator"
${KUBECTL} create namespace kafka
${KUBECTL} apply -f https://strimzi.io/install/latest?namespace=kafka -n kafka
${KUBECTL} wait --for=condition=ready pod -l name=strimzi-cluster-operator -n kafka --timeout=300s

info "Applying sidecar injection demo manifests"
${KUBECTL} apply -f "${DIST_DIR}/examples/sidecar-injection-demo/01-namespace.yaml"
${KUBECTL} apply -f "${DIST_DIR}/examples/sidecar-injection-demo/02-kafka.yaml"
${KUBECTL} apply -f "${DIST_DIR}/examples/sidecar-injection-demo/03-sidecar-config.yaml"
${KUBECTL} apply -f "${DIST_DIR}/examples/sidecar-injection-demo/04-app.yaml"

info "Waiting for Kafka cluster to be ready"
${KUBECTL} wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka

info "Waiting for message-producer pod to be ready"
${KUBECTL} wait --for=condition=ready pod -l app=message-producer -n demo-app --timeout=300s

echo -e "${GREEN}Installation complete!${NO_COLOUR}"
echo ""
cat << EOF
Verify sidecar injection:
  kubectl get pod -l app=message-producer -n demo-app -o jsonpath='{.items[0].spec.containers[*].name}'
  kubectl get pod -l app=message-producer -n demo-app -o jsonpath='{.items[0].spec.initContainers[*].name}'
Expected: producer, kroxylicious-proxy
Consume messages (should be UPPERCASE):
  kubectl exec -n kafka my-cluster-dual-role-0 -- \\
    /opt/kafka/bin/kafka-console-consumer.sh \\
    --bootstrap-server localhost:9092 \\
    --topic demo-topic \\
    --from-beginning \\
    --max-messages 5
EOF
