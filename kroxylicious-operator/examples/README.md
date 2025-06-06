# Examples

These examples assume a Kafka cluster running the Kubernetes Cluster.

The [Strimzi Quickstart](https://strimzi.io/quickstarts/) fulfils the requirement:

## Install Strimzi

```
kubectl create ns kafka
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml -n kafka
kubectl wait deployment/strimzi-cluster-operator --for=condition=Available=True --timeout=300s -n kafka
```

## Deploy the Quickstart

```
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml -n kafka
kubectl wait -n kafka kafka my-cluster --for=condition=Ready=True
```
