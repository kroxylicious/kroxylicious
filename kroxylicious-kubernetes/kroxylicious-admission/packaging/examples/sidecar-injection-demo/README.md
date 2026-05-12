# Sidecar Injection Demo: Message Uppercasing

This example demonstrates the Kroxylicious admission webhook by injecting a sidecar proxy that transforms Kafka messages to uppercase.

## Prerequisites

- Kubernetes cluster (1.28+ for native sidecar support)
- `kubectl` configured to access your cluster
- Kroxylicious admission webhook installed (see [../install/](../../install/))

## Architecture

The example creates:

1. A namespace (`demo-app`) with sidecar injection enabled
2. A single-node Strimzi Kafka cluster
3. A `KroxyliciousSidecarConfig` that configures message uppercasing
4. An application that sends lowercase messages to Kafka

The admission webhook intercepts pod creation in the `demo-app` namespace and injects a Kroxylicious sidecar container. The sidecar applies two filters:

- `ProduceRequestTransformation` with `UpperCasing` - transforms produced messages to uppercase
- `FetchResponseTransformation` with `Replacing` - transforms fetched messages by replacing 'O' with '❤️'

Application containers automatically have `KAFKA_BOOTSTRAP_SERVERS` set to `localhost:9092`, pointing to the sidecar proxy.

## Installation

### 1. Install Strimzi Operator

```bash
kubectl create namespace kafka
kubectl apply -f https://strimzi.io/install/latest?namespace=kafka -n kafka
```

Wait for the operator to be ready:

```bash
kubectl wait --for=condition=ready pod -l name=strimzi-cluster-operator -n kafka --timeout=300s
```

### 2. Apply the Demo Manifests

```bash
kubectl apply -f 01-namespace.yaml
kubectl apply -f 02-kafka.yaml
kubectl apply -f 03-sidecar-config.yaml
```

Wait for the sidecar configuration to be accepted:

```bash
kubectl wait kroxylicioussidecarconfig/demo-config --for=condition=Ready -n demo-app --timeout=60s
```

Apply the application:

```bash
kubectl apply -f 04-app.yaml
```

### 3. Wait for Resources to be Ready

Wait for Kafka cluster:

```bash
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
```

Wait for the application pod:

```bash
kubectl wait --for=condition=ready pod -l app=message-producer -n demo-app --timeout=300s
```

## Verification

### Verify Sidecar Injection

Check that the application pod has a `kroxylicious-proxy` sidecar container injected:

```bash
kubectl get pod -l app=message-producer -n demo-app -o jsonpath='{.items[0].spec.containers[*].name}'
kubectl get pod -l app=message-producer -n demo-app -o jsonpath='{.items[0].spec.initContainers[*].name}'
```

Expected output: `producer kroxylicious-proxy`

### Verify Environment Variable Injection

Check that `KAFKA_BOOTSTRAP_SERVERS` was set by the webhook:

```bash
kubectl exec -n demo-app deployment/message-producer -c producer -- env | grep KAFKA_BOOTSTRAP_SERVERS
```

Expected output: `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`

### View Producer Logs

The producer application sends lowercase messages like `hello world from kubernetes at <timestamp>`:

```bash
kubectl logs -n demo-app deployment/message-producer -c producer --tail=20
```

### Consume Messages and See Uppercase Transformation

Run a consumer inside the Kafka cluster (bypassing the proxy) to see messages as stored in Kafka:

```bash
kubectl exec -n kafka my-cluster-dual-role-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic demo-topic \
    --from-beginning \
    --max-messages 5
```

Expected output shows **uppercase** messages:

```
HELLO WORLD FROM KUBERNETES AT MON MAY  5 12:34:56 UTC 2026
HELLO WORLD FROM KUBERNETES AT MON MAY  5 12:35:01 UTC 2026
...
```

The producer sent lowercase messages, but the `ProduceRequestTransformation` filter uppercased them before they reached Kafka.

### Consume via the Proxy

Consume messages from the application pod (via the sidecar proxy):

```bash
kubectl exec -n demo-app deployment/message-producer -c producer -- \
  bash -c '/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
    --topic demo-topic \
    --from-beginning \
    --max-messages 5'
```

You should see the uppercase content sent to the target cluster, but with 'O's replaced with '❤️ 's.

```
HELL❤️ W❤️RLD FR❤️M KUBERNETES AT WED MAY  6 23:43:10 UTC 2026
HELL❤️ W❤️RLD FR❤️M KUBERNETES AT WED MAY  6 23:43:16 UTC 2026
HELL❤️ W❤️RLD FR❤️M KUBERNETES AT WED MAY  6 23:43:22 UTC 2026
HELL❤️ W❤️RLD FR❤️M KUBERNETES AT WED MAY  6 23:43:28 UTC 2026
HELL❤️ W❤️RLD FR❤️M KUBERNETES AT WED MAY  6 23:43:34 UTC 2026
```

The target cluster contains uppercase messages, but the `FetchResponseTransformation` filter replaced the 'O's with '❤️ 's before they are forwarded to the client.

## Cleanup

```bash
kubectl delete namespace demo-app
```
