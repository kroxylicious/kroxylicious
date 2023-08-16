# Performance

This document describes how to run some basic performance tests for the proxy.

## Preparation

Download Apache Kafka:

```shell
wget https://downloads.apache.org/kafka/3.4.1/kafka_2.13-3.4.1.tgz
tar xvf kafka_2.13-3.4.1.tgz
```

Start up ZooKeeper and Kafka:

```shell
cd kafka_2.13-3.4.1
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

## Execution

Build and launch the proxy:

```shell
mvn clean verify -Pdist -Dquick
kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh --config kroxylicious-app/example-proxy-config.yml
```

Run Kafka's _kafka-producer-perf-test.sh_ script:

```shell
bin/kafka-producer-perf-test.sh \
--topic perf-test \
--throughput -1 \
--num-records 10000000 \
--record-size 1024 \
--producer-props acks=all bootstrap.servers=localhost:9192
```

Run once to allow for JVM warm-up, then run another time for getting meaningful values.
This produces large volumes of Kafka logs in _/tmp/kafka-logs_ which should removed after stopping Kafka.

## Results

Results from a benchmark run on Lenovo T14s.

### Direct Kafka

These values are for connecting to Kafka directly.

```
339458 records sent, 67891.6 records/sec (66.30 MB/sec), 0.7 ms avg latency, 41.0 ms max latency.
340690 records sent, 68138.0 records/sec (66.54 MB/sec), 0.4 ms avg latency, 8.0 ms max latency.
328872 records sent, 65774.4 records/sec (64.23 MB/sec), 3.0 ms avg latency, 166.0 ms max latency.
350133 records sent, 70026.6 records/sec (68.39 MB/sec), 0.4 ms avg latency, 3.0 ms max latency.
352684 records sent, 70536.8 records/sec (68.88 MB/sec), 0.5 ms avg latency, 33.0 ms max latency.
345508 records sent, 69101.6 records/sec (67.48 MB/sec), 1.3 ms avg latency, 63.0 ms max latency.
347900 records sent, 69580.0 records/sec (67.95 MB/sec), 1.5 ms avg latency, 79.0 ms max latency.
313252 records sent, 62650.4 records/sec (61.18 MB/sec), 69.4 ms avg latency, 592.0 ms max latency.
328890 records sent, 65778.0 records/sec (64.24 MB/sec), 3.2 ms avg latency, 177.0 ms max latency.
330865 records sent, 66173.0 records/sec (64.62 MB/sec), 0.6 ms avg latency, 19.0 ms max latency.
326143 records sent, 65228.6 records/sec (63.70 MB/sec), 0.6 ms avg latency, 14.0 ms max latency.
331257 records sent, 66251.4 records/sec (64.70 MB/sec), 3.6 ms avg latency, 222.0 ms max latency.
326291 records sent, 65258.2 records/sec (63.73 MB/sec), 0.8 ms avg latency, 40.0 ms max latency.
316958 records sent, 63391.6 records/sec (61.91 MB/sec), 0.5 ms avg latency, 13.0 ms max latency.
342797 records sent, 68559.4 records/sec (66.95 MB/sec), 1.9 ms avg latency, 122.0 ms max latency.
334420 records sent, 66884.0 records/sec (65.32 MB/sec), 1.9 ms avg latency, 125.0 ms max latency.
302166 records sent, 60433.2 records/sec (59.02 MB/sec), 1.3 ms avg latency, 36.0 ms max latency.
300626 records sent, 59802.3 records/sec (58.40 MB/sec), 1.8 ms avg latency, 95.0 ms max latency.
320412 records sent, 64082.4 records/sec (62.58 MB/sec), 2.6 ms avg latency, 186.0 ms max latency.
345146 records sent, 69029.2 records/sec (67.41 MB/sec), 0.6 ms avg latency, 19.0 ms max latency.
348797 records sent, 69759.4 records/sec (68.12 MB/sec), 0.6 ms avg latency, 36.0 ms max latency.
10000000 records sent, 68342.422876 records/sec (66.74 MB/sec), 3.92 ms avg latency, 592.00 ms max latency, 0 ms 50th, 1 ms 95th, 116 ms 99th, 534 ms 99.9th.
```

### Proxy - pass-through

These values show the minimal overhead of the proxy; it doesn't apply any modifications
other than adjusting the advertised host and negotiating API versions.

```
323930 records sent, 64786.0 records/sec (63.27 MB/sec), 0.4 ms avg latency, 6.0 ms max latency.
300981 records sent, 60196.2 records/sec (58.79 MB/sec), 2.3 ms avg latency, 158.0 ms max latency.
328703 records sent, 65740.6 records/sec (64.20 MB/sec), 0.4 ms avg latency, 5.0 ms max latency.
329130 records sent, 65826.0 records/sec (64.28 MB/sec), 0.4 ms avg latency, 6.0 ms max latency.
309969 records sent, 61993.8 records/sec (60.54 MB/sec), 2.1 ms avg latency, 192.0 ms max latency.
328791 records sent, 65758.2 records/sec (64.22 MB/sec), 0.4 ms avg latency, 5.0 ms max latency.
325668 records sent, 65133.6 records/sec (63.61 MB/sec), 0.4 ms avg latency, 4.0 ms max latency.
319086 records sent, 63817.2 records/sec (62.32 MB/sec), 3.5 ms avg latency, 198.0 ms max latency.
316409 records sent, 63281.8 records/sec (61.80 MB/sec), 0.4 ms avg latency, 7.0 ms max latency.
327035 records sent, 65407.0 records/sec (63.87 MB/sec), 0.4 ms avg latency, 4.0 ms max latency.
329362 records sent, 65872.4 records/sec (64.33 MB/sec), 0.4 ms avg latency, 4.0 ms max latency.
312351 records sent, 62470.2 records/sec (61.01 MB/sec), 7.3 ms avg latency, 269.0 ms max latency.
331314 records sent, 66262.8 records/sec (64.71 MB/sec), 0.4 ms avg latency, 4.0 ms max latency.
328897 records sent, 65779.4 records/sec (64.24 MB/sec), 0.4 ms avg latency, 4.0 ms max latency.
307341 records sent, 61468.2 records/sec (60.03 MB/sec), 4.0 ms avg latency, 228.0 ms max latency.
326419 records sent, 65283.8 records/sec (63.75 MB/sec), 0.4 ms avg latency, 7.0 ms max latency.
329697 records sent, 65939.4 records/sec (64.39 MB/sec), 0.4 ms avg latency, 3.0 ms max latency.
320019 records sent, 64003.8 records/sec (62.50 MB/sec), 5.0 ms avg latency, 166.0 ms max latency.
318647 records sent, 63729.4 records/sec (62.24 MB/sec), 1.7 ms avg latency, 117.0 ms max latency.
319843 records sent, 63968.6 records/sec (62.47 MB/sec), 0.9 ms avg latency, 35.0 ms max latency.
287973 records sent, 57594.6 records/sec (56.24 MB/sec), 6.6 ms avg latency, 281.0 ms max latency.
310381 records sent, 62076.2 records/sec (60.62 MB/sec), 0.7 ms avg latency, 18.0 ms max latency.
311969 records sent, 62393.8 records/sec (60.93 MB/sec), 0.6 ms avg latency, 9.0 ms max latency.
311976 records sent, 62395.2 records/sec (60.93 MB/sec), 0.6 ms avg latency, 9.0 ms max latency.
291281 records sent, 58256.2 records/sec (56.89 MB/sec), 11.2 ms avg latency, 329.0 ms max latency.
311064 records sent, 62212.8 records/sec (60.75 MB/sec), 0.6 ms avg latency, 10.0 ms max latency.
10000000 records sent, 64342.197543 records/sec (62.83 MB/sec), 9.33 ms avg latency, 635.00 ms max latency, 0 ms 50th, 1 ms 95th, 431 ms 99th, 561 ms 99.9th.
```

### Proxy - Lower-case transform

These values show the overhead of an actual message transformation; each produce request
(we assume it's UTF-8 text) is lower-cased:

```
...
new ProduceRecordTransformationFilter(
    buffer -> ByteBuffer.wrap(new String(StandardCharsets.UTF_8.decode(buffer).array()).toLowerCase().getBytes(StandardCharsets.UTF_8))
)
...
```

```
287479 records sent, 57495.8 records/sec (56.15 MB/sec), 2.8 ms avg latency, 34.0 ms max latency.
288400 records sent, 57680.0 records/sec (56.33 MB/sec), 1.6 ms avg latency, 17.0 ms max latency.
298933 records sent, 59786.6 records/sec (58.39 MB/sec), 2.3 ms avg latency, 164.0 ms max latency.
254990 records sent, 50998.0 records/sec (49.80 MB/sec), 173.1 ms avg latency, 414.0 ms max latency.
273600 records sent, 54720.0 records/sec (53.44 MB/sec), 20.4 ms avg latency, 144.0 ms max latency.
304748 records sent, 60949.6 records/sec (59.52 MB/sec), 2.4 ms avg latency, 32.0 ms max latency.
280629 records sent, 56125.8 records/sec (54.81 MB/sec), 18.5 ms avg latency, 264.0 ms max latency.
306855 records sent, 61358.7 records/sec (59.92 MB/sec), 125.8 ms avg latency, 427.0 ms max latency.
323123 records sent, 64624.6 records/sec (63.11 MB/sec), 2.4 ms avg latency, 50.0 ms max latency.
300819 records sent, 60163.8 records/sec (58.75 MB/sec), 19.2 ms avg latency, 287.0 ms max latency.
292274 records sent, 58454.8 records/sec (57.08 MB/sec), 4.8 ms avg latency, 58.0 ms max latency.
301514 records sent, 60302.8 records/sec (58.89 MB/sec), 1.4 ms avg latency, 34.0 ms max latency.
289573 records sent, 57914.6 records/sec (56.56 MB/sec), 2.8 ms avg latency, 58.0 ms max latency.
271608 records sent, 54321.6 records/sec (53.05 MB/sec), 25.4 ms avg latency, 294.0 ms max latency.
291544 records sent, 58308.8 records/sec (56.94 MB/sec), 1.6 ms avg latency, 18.0 ms max latency.
284910 records sent, 56982.0 records/sec (55.65 MB/sec), 7.3 ms avg latency, 81.0 ms max latency.
268745 records sent, 53749.0 records/sec (52.49 MB/sec), 25.7 ms avg latency, 378.0 ms max latency.
280286 records sent, 56057.2 records/sec (54.74 MB/sec), 2.7 ms avg latency, 59.0 ms max latency.
287880 records sent, 57576.0 records/sec (56.23 MB/sec), 1.5 ms avg latency, 16.0 ms max latency.
10000000 records sent, 60679.611650 records/sec (59.26 MB/sec), 25.00 ms avg latency, 1080.00 ms max latency, 1 ms 50th, 170 ms 95th, 601 ms 99th, 992 ms 99.9th.

```

### Jenkins pipeline for performance

When a PR is created and the performance tests are needed, if you are a member of
[Developers](https://github.com/orgs/kroxylicious/teams/developers), you may add the following comment into the PR to trigger the run.

```
@strimzi-ci run perf
```

It will launch the `kroxylicious-performance-tests-pr` build, that will insert a comment with a summary into the PR comparing the results with the previous execution.

In case a manual execution needs to be done, this job is ready for that `kroxylicious-performance-tests`, where
some parameters can be configurable (NUMBER_OF_MESSAGES, MESSAGE_SIZE, PRODUCER_PROPERTIES)
