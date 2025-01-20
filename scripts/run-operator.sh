#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# simple script to build and run the operator
cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" || exit
cd .. || exit

if ! minikube status
then
  echo "starting minikube"
  minikube start
fi

echo "install cert-manager"
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.0/cert-manager.yaml
kubectl wait deployment/cert-manager-webhook --for=condition=Available=True --timeout=300s -n cert-manager

echo "building operator image in minikube"
KROXYLICIOUS_VERSION=${KROXYLICIOUS_VERSION:-$(mvn org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=project.version -q -DforceStdout)}
minikube image build . -f Dockerfile.operator -t quay.io/kroxylicious/operator:latest --build-opt=build-arg=KROXYLICIOUS_VERSION="${KROXYLICIOUS_VERSION}"

cd kroxylicious-operator || exit
echo "installing kafka (no-op if already installed)"
kubectl create namespace kafka
kubectl create -n kafka -f 'https://strimzi.io/install/latest?namespace=kafka'
kubectl wait -n kafka deployment/strimzi-cluster-operator --for=condition=Available=True --timeout=300s
kubectl apply -n kafka -f https://strimzi.io/examples/latest/kafka/kraft/kafka-single-node.yaml
kubectl wait -n kafka kafka/my-cluster --for=condition=Ready --timeout=300s

echo "deleting example"
kubectl delete -f examples/simple/ --ignore-not-found=true --timeout=30s --grace-period=1

echo "deleting kroxylicious-operator installation"
kubectl delete -n kroxylicious-operator all --all --timeout=30s --grace-period=1
kubectl delete -f install --ignore-not-found=true --timeout=30s --grace-period=1

echo "deleting all kroxylicious.io resources and crds"
for crd in $(kubectl get crds -oname | grep kroxylicious.io | awk -F / '{ print $2 }');
  do
  export crd
  echo "deleting resources for crd: $crd"
  kubectl delete -A --all "$(kubectl get crd "${crd}" -o=jsonpath='{.spec.names.singular}')" --timeout=30s --grace-period=1
  echo "deleting crd: ${crd}"
  kubectl delete crd "${crd}"
done

echo "installing crds"
kubectl apply -f src/main/resources/META-INF/fabric8
echo "installing kroxylicious-operator"
kubectl apply -f install
echo "installing simple proxy"
kubectl apply -f examples/simple/

if kubectl wait -n my-proxy kafkaproxy/simple --for=condition=Ready=True --timeout=300s
then
  echo "simple proxy should now be available in-cluster at my-cluster.my-proxy.svc.cluster.local:9292"
else
  echo "something went wrong!"
  exit 1
fi

echo 'to produce'
echo 'kubectl exec -it -nkafka my-cluster-dual-role-0 -- bash -c "echo $(kubectl get secret -n my-proxy kroxy-server-key-material -o json | jq -r ".data.\"tls.crt\"") | base64 -d > /tmp/certo && ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-bootstrap.my-proxy.svc.cluster.local:9092 --topic my-topic --producer-property ssl.truststore.type=PEM --producer-property security.protocol=SSL --producer-property ssl.truststore.location=/tmp/certo"'

echo 'to consume'
echo 'kubectl exec -it -nkafka my-cluster-dual-role-0 -- bash -c "echo $(kubectl get secret -n my-proxy kroxy-server-key-material -o json | jq -r ".data.\"tls.crt\"") | base64 -d > /tmp/certo && ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-bootstrap.my-proxy.svc.cluster.local:9092 --topic my-topic --from-beginning --consumer-property ssl.truststore.type=PEM --consumer-property security.protocol=SSL --consumer-property ssl.truststore.location=/tmp/certo"'
