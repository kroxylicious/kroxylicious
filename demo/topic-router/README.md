# Topic Router Demo

This demo runs two independent 3-broker Kafka clusters and a Kroxylicious proxy
in Docker.  The topic router splits traffic by topic name prefix:

- `a.*` topics -> **cluster-a** (default route)
- `b.*` topics -> **cluster-b**

It demonstrates non-transactional produce routing, cross-cluster consume,
transactional produce, and read-committed consumer groups.

## Architecture

```
                          +---------------------------+
                          |     Kroxylicious Proxy    |
 Client ----SASL--------> |  (kroxylicious:9192)      |
   (on Docker network)    |  SaslInspection filter    |
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

          All containers on the "kroxylicious-demo" Docker network
```

SASL PLAIN authentication identifies users for subject-based routing.
User `bob` is subject-routed to `route-b`, so all his coordinator-bound
operations (transactions, consumer groups) go to cluster-b.

Aside: The presence of _some_ authentication is required for the Subject-based routing
used to avoid the need for cross-cluster transactions. SASL termination at the proxy 
is the right way to do this (because, in general, you cannot re-use a SASL exchange 
with multiple brokers). However, the project currently doesn't really support any SASL 
termination filters, so using SaslInspection forwarding only to the default cluster is 
a temporary hack.

## Prerequisites

- **Java 21+**
- **Docker** (or Podman) with Docker Compose

All Kafka CLI commands in this demo run inside the Kafka Docker image,
so you do not need a local Kafka installation.

All commands below assume you are in the repository root.

Throughout this guide, replace `podman` with `docker` if you use Docker.

## Helper alias

To avoid repeating the full `podman run` invocation, define a shell function.
All containers join the `kroxylicious-demo` network so they can reach the
brokers and proxy by hostname:

```bash
PROXY=kroxylicious:9192
CLUSTER_A=kafka-a-1:9094    # PLAINTEXT listener for direct access
CLUSTER_B=kafka-b-1:9093    # PLAINTEXT listener for direct access

kafka-cmd() {
  podman run --rm -i --network kroxylicious-demo docker.io/apache/kafka:4.2.0 \
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

### 2. Load the proxy image

```bash
podman load < kroxylicious-app/target/kroxylicious-proxy.img.tar.gz
```

### 3. Start the clusters and proxy

```bash
podman compose -f demo/topic-router/docker-compose.yaml up -d
```

Wait for both clusters to be ready (may take 15-20 seconds):

```bash
kafka-cmd kafka-topics.sh --bootstrap-server $CLUSTER_A --list
kafka-cmd kafka-topics.sh --bootstrap-server $CLUSTER_B --list
```

Both commands should return without error (empty list is fine).

Check that the proxy is running:

```bash
podman compose -f demo/topic-router/docker-compose.yaml logs kroxylicious | tail -5
```

### 4. Create topics

Create topics directly on each cluster via their PLAINTEXT listeners:

```bash
kafka-cmd kafka-topics.sh --bootstrap-server $CLUSTER_A \
  --create --topic a.orders --partitions 3

kafka-cmd kafka-topics.sh --bootstrap-server $CLUSTER_B \
  --create --topic b.analytics --partitions 3
```

## Demo 1: Non-transactional produce routing

Produce to topics with different prefixes through the proxy.
Records land on different backend clusters.

```bash
# Produce to a.orders -- routes to cluster-a
echo "order:order-1" | kafka-cmd-alice kafka-console-producer.sh \
  --bootstrap-server $PROXY \
  --topic a.orders \
  --reader-property parse.key=true --reader-property key.separator=:

# Produce to b.analytics -- routes to cluster-b
echo "event:event-1" | kafka-cmd-alice kafka-console-producer.sh \
  --bootstrap-server $PROXY \
  --topic b.analytics \
  --reader-property parse.key=true --reader-property key.separator=:
```

Check offsets directly on each cluster to confirm records landed on the
right cluster.  The keys `order` and `event` both hash to partition 2
under Kafka's default murmur2 partitioner with 3 partitions.

```bash
kafka-cmd kafka-get-offsets.sh --bootstrap-server $CLUSTER_A --topic a.orders
# Expected: a.orders:2:1  (partition 2 has one record)

kafka-cmd kafka-get-offsets.sh --bootstrap-server $CLUSTER_B --topic b.analytics
# Expected: b.analytics:2:1  (partition 2 has one record)
```

Verify the record contents:

```bash
kafka-cmd kafka-console-consumer.sh --bootstrap-server $CLUSTER_A \
  --topic a.orders --partition 2 --from-beginning --max-messages 1

kafka-cmd kafka-console-consumer.sh --bootstrap-server $CLUSTER_B \
  --topic b.analytics --partition 2 --from-beginning --max-messages 1
```

## Demo 2: Cross-cluster consume

Consume records from each cluster individually through the proxy using manual
partition assignment (`--partition 2`). This bypasses the group coordinator,
which currently cannot route across clusters for ungrouped consumers.

```bash
# a.orders (routed to cluster-a)
kafka-cmd-alice kafka-console-consumer.sh \
  --bootstrap-server $PROXY \
  --topic a.orders --partition 2 \
  --from-beginning --max-messages 1

# b.analytics (routed to cluster-b)
kafka-cmd-alice kafka-console-consumer.sh \
  --bootstrap-server $PROXY \
  --topic b.analytics --partition 2 \
  --from-beginning --max-messages 1
```

## Demo 3: Transactional produce

User `bob` is subject-routed to `route-b`, so his transaction
coordinator lives on cluster-b.

```bash
podman run --rm --network kroxylicious-demo docker.io/apache/kafka:4.2.0 sh -c '
cat > /tmp/bob-txn.properties << '\''PROPS'\''
bootstrap.servers=kroxylicious:9192
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

Check offsets — one partition should show 6 additional records (5 data + 1 txn marker):

```bash
kafka-cmd kafka-get-offsets.sh --bootstrap-server $CLUSTER_B --topic b.analytics
```

Verify with a `read_committed` consumer directly on cluster-b:

```bash
kafka-cmd kafka-console-consumer.sh --bootstrap-server $CLUSTER_B \
  --topic b.analytics --from-beginning \
  --isolation-level read_committed
```

(Press Ctrl+C to stop the consumer after seeing the records.)

## Demo 4: Read-committed consumer group through the proxy

Bob consumes the transactionally-produced records through the proxy using a
consumer group with the new consumer group protocol (KIP-848).
His consumer group coordinator routes to cluster-b
(via `subjects` in the proxy config).

```bash
kafka-cmd-bob kafka-console-consumer.sh \
  --bootstrap-server $PROXY \
  --topic b.analytics --from-beginning \
  --group bob-consumer-group \
  --isolation-level read_committed \
  --command-property group.protocol=consumer
```

(Press Ctrl+C after seeing the records.)

Verify the consumer group exists on cluster-b but not cluster-a:

```bash
# Group exists on cluster-b
kafka-cmd kafka-consumer-groups.sh --bootstrap-server $CLUSTER_B \
  --describe --group bob-consumer-group

# Group does NOT exist on cluster-a
kafka-cmd kafka-consumer-groups.sh --bootstrap-server $CLUSTER_A \
  --describe --group bob-consumer-group
```

## Teardown

```bash
podman compose -f demo/topic-router/docker-compose.yaml down -v
```
