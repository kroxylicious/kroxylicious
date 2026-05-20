# Topic Router Demo

This demo runs two independent 3-broker Kafka clusters behind a single Kroxylicious proxy.
The topic router splits traffic by topic name prefix:

- `a.*` topics -> **cluster-a** (default route)
- `b.*` topics -> **cluster-b**

It demonstrates non-transactional produce routing, cross-cluster consume,
transactional produce, and read-committed consumer groups.

## Architecture

```
                          +---------------------------+
                          |     Kroxylicious Proxy    |
 Client --SASL_PLAIN--->  |  (localhost:9192)         |
                          |  SaslInspection filter    |
                          |  TopicPartitionRouter     |
                          +---+-----------------+-----+
                              |                 |
                    SASL_PLAINTEXT          PLAINTEXT
                              |                 |
                    +---------v--+    +---------v--+
                    | Cluster A  |    | Cluster B  |
                    | (default)  |    |            |
                    | 3 brokers  |    | 3 brokers  |
                    | a.* topics |    | b.* topics |
                    +------------+    +------------+
```

SASL PLAIN authentication identifies users for subject-based routing of
transactions and consumer groups. User `bob` is mapped to `route-b` for both.

## Prerequisites

- **Java 21+**
- **Docker** (or Podman) with Docker Compose

All Kafka CLI commands in this demo run inside the Kafka Docker image,
so you do not need a local Kafka installation.

All commands below assume you are in the repository root.

Throughout this guide, replace `podman` with `docker` if you use Docker.

## Helper alias

To avoid repeating the full `podman run` invocation, define a shell function:

```bash
kafka-cmd() {
  podman run --rm --network host docker.io/apache/kafka:4.2.0 \
    /opt/kafka/bin/"$@"
}
```

Commands that go through the proxy need SASL credentials. Define helpers
for each user:

```bash
kafka-cmd-alice() {
  kafka-cmd "$1" "${@:2}" \
    --command-property security.protocol=SASL_PLAINTEXT \
    --command-property sasl.mechanism=PLAIN \
    --command-property 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="alice-secret";'
}

kafka-cmd-bob() {
  kafka-cmd "$1" "${@:2}" \
    --command-property security.protocol=SASL_PLAINTEXT \
    --command-property sasl.mechanism=PLAIN \
    --command-property 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="bob" password="bob-secret";'
}
```

## Setup

### 1. Build Kroxylicious

```bash
mvn clean package -Pdist -Dquick
```

### 2. Start Kafka clusters

```bash
podman compose -f demo/topic-router/docker-compose.yaml up -d
```

Wait for both clusters to be ready (may take 15-20 seconds):

```bash
# Cluster A (PLAINTEXT listener on port 19192)
kafka-cmd kafka-topics.sh --bootstrap-server localhost:19192 --list

# Cluster B (PLAINTEXT listener on port 29092)
kafka-cmd kafka-topics.sh --bootstrap-server localhost:29092 --list
```

Both commands should return without error (empty list is fine).

### 3. Create topics

Create topics directly on each cluster via their PLAINTEXT listeners:

```bash
kafka-cmd kafka-topics.sh --bootstrap-server localhost:19192 \
  --create --topic a.orders --partitions 3

kafka-cmd kafka-topics.sh --bootstrap-server localhost:29092 \
  --create --topic b.analytics --partitions 3
```

### 4. Start the proxy

The topic router module is not bundled in `kroxylicious-app` by default.
Use `KROXYLICIOUS_CLASSPATH` to add it at runtime:

```bash
KROXYLICIOUS_CLASSPATH="$(pwd)/kroxylicious-router-topic/target/kroxylicious-router-topic-*.jar:$(pwd)/kroxylicious-kafka-message-tools/target/kroxylicious-kafka-message-tools-*.jar" \
  kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh \
  --config demo/topic-router/proxy-config.yaml
```

The proxy listens on `localhost:9192` (bootstrap) with broker ports 9193-9201.

Leave this running in a separate terminal.

## Demo 1: Non-transactional produce routing

Produce to topics with different prefixes through the proxy.
Records land on different backend clusters.

```bash
# Produce to a.orders -- routes to cluster-a
echo "order-1" | kafka-cmd-alice kafka-console-producer.sh \
  --bootstrap-server localhost:9192 \
  --topic a.orders

# Produce to b.analytics -- routes to cluster-b
echo "event-1" | kafka-cmd-alice kafka-console-producer.sh \
  --bootstrap-server localhost:9192 \
  --topic b.analytics
```

Verify directly on each cluster (PLAINTEXT, no authentication needed):

```bash
# Records on cluster-a
kafka-cmd kafka-console-consumer.sh --bootstrap-server localhost:19192 \
  --topic a.orders --from-beginning --max-messages 1

# Records on cluster-b
kafka-cmd kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic b.analytics --from-beginning --max-messages 1
```

## Demo 2: Cross-cluster consume

A single consumer through the proxy sees records from topics on both clusters:

```bash
kafka-cmd-alice kafka-console-consumer.sh \
  --bootstrap-server localhost:9192 \
  --include "a.orders|b.analytics" \
  --from-beginning --max-messages 2
```

## Demo 3: Transactional produce

User `bob` is mapped to `route-b` for transactions, so his transaction
coordinator lives on cluster-b.

```bash
podman run --rm --network host docker.io/apache/kafka:4.2.0 sh -c '
cat > /tmp/bob-txn.properties << '\''PROPS'\''
bootstrap.servers=localhost:9192
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="bob" password="bob-secret";
transactional.id=demo-txn
PROPS
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic b.analytics \
  --throughput 1 --num-records 5 --record-size 100 \
  --command-config /tmp/bob-txn.properties
'
```

Verify with a `read_committed` consumer directly on cluster-b:

```bash
kafka-cmd kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic b.analytics --from-beginning \
  --isolation-level read_committed
```

(Press Ctrl+C to stop the consumer after seeing the records.)

## Demo 4: Read-committed consumer group through the proxy

Bob consumes the transactionally-produced records through the proxy using a
consumer group with the new consumer group protocol (KIP-848).
His consumer group coordinator routes to cluster-b
(via `consumerGroupUserRoutes` in the proxy config).

```bash
kafka-cmd-bob kafka-console-consumer.sh \
  --bootstrap-server localhost:9192 \
  --topic b.analytics --from-beginning \
  --group bob-consumer-group \
  --isolation-level read_committed \
  --command-property group.protocol=consumer
```

(Press Ctrl+C after seeing the records.)

Verify the consumer group exists on cluster-b but not cluster-a:

```bash
# Group exists on cluster-b
kafka-cmd kafka-consumer-groups.sh --bootstrap-server localhost:29092 \
  --describe --group bob-consumer-group

# Group does NOT exist on cluster-a
kafka-cmd kafka-consumer-groups.sh --bootstrap-server localhost:19192 \
  --describe --group bob-consumer-group
```

## Teardown

Stop the proxy with Ctrl+C, then tear down the Kafka clusters:

```bash
podman compose -f demo/topic-router/docker-compose.yaml down -v
```
