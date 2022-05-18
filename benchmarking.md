# Benchmarking

This document describes how to run some basic performance tests for the proxy.

## Preparation

Download Apache Kafka:

```
$ wget https://downloads.apache.org/kafka/3.1.0/kafka_2.13-3.1.0.tgz
$ tar xvf kafka_2.13-3.1.0.tgz
```

Start up ZooKeeper and Kafka:

```
$ cd kafka_2.13-3.1.0
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin/kafka-server-start.sh config/server.properties
```

## Execution

Build and launch the proxy:

```
$ mvn clean verify -Pdist -Dquick
$ java -jar target/kproxy-1.0-SNAPSHOT.jar
```

Run Kafka's _kafka-producer-perf-test.sh_ script:

```
$ bin/kafka-producer-perf-test.sh \
--topic perf-test \
--throughput -1 \
--num-records 10000000 \
--record-size 1024 \
--producer-props acks=all bootstrap.servers=localhost:9192
```

Run once to allow for JVM warm-up, then run another time for getting meaningful values.
This produces large volumes of Kafka logs in _/tmp/kafka-logs_ which should removed after stopping Kafka.

## Results

Results from a benchmark run on MacMini M1.

### Direct Kafka

These values are for connecting to Kafka directly.

```
477187 records sent, 95437,4 records/sec (93,20 MB/sec), 0,4 ms avg latency, 149,0 ms max latency.
507616 records sent, 101523,2 records/sec (99,14 MB/sec), 0,8 ms avg latency, 54,0 ms max latency.
506905 records sent, 101381,0 records/sec (99,00 MB/sec), 0,3 ms avg latency, 18,0 ms max latency.
504944 records sent, 100988,8 records/sec (98,62 MB/sec), 0,7 ms avg latency, 52,0 ms max latency.
508329 records sent, 101665,8 records/sec (99,28 MB/sec), 0,3 ms avg latency, 18,0 ms max latency.
507461 records sent, 101492,2 records/sec (99,11 MB/sec), 0,7 ms avg latency, 55,0 ms max latency.
508605 records sent, 101721,0 records/sec (99,34 MB/sec), 0,3 ms avg latency, 18,0 ms max latency.
509013 records sent, 101802,6 records/sec (99,42 MB/sec), 1,0 ms avg latency, 61,0 ms max latency.
502944 records sent, 100588,8 records/sec (98,23 MB/sec), 0,4 ms avg latency, 26,0 ms max latency.
483576 records sent, 96715,2 records/sec (94,45 MB/sec), 1,1 ms avg latency, 58,0 ms max latency.
503447 records sent, 100689,4 records/sec (98,33 MB/sec), 0,6 ms avg latency, 42,0 ms max latency.
508601 records sent, 101720,2 records/sec (99,34 MB/sec), 0,6 ms avg latency, 55,0 ms max latency.
507329 records sent, 101465,8 records/sec (99,09 MB/sec), 0,3 ms avg latency, 18,0 ms max latency.
506374 records sent, 101274,8 records/sec (98,90 MB/sec), 1,0 ms avg latency, 56,0 ms max latency.
504911 records sent, 100982,2 records/sec (98,62 MB/sec), 0,4 ms avg latency, 27,0 ms max latency.
504096 records sent, 100819,2 records/sec (98,46 MB/sec), 0,7 ms avg latency, 54,0 ms max latency.
507512 records sent, 101502,4 records/sec (99,12 MB/sec), 0,3 ms avg latency, 18,0 ms max latency.
505010 records sent, 101002,0 records/sec (98,63 MB/sec), 0,7 ms avg latency, 53,0 ms max latency.
504117 records sent, 100823,4 records/sec (98,46 MB/sec), 0,3 ms avg latency, 19,0 ms max latency.
10000000 records sent, 100731,309306 records/sec (98,37 MB/sec), 0,56 ms avg latency, 149,00 ms max latency, 0 ms 50th, 1 ms 95th, 13 ms 99th, 47 ms 99.9th.
```

### Proxy - pass-through

These values show the minimal overhead of the proxy; it doesn't apply any modifications
other than adjusting the advertised host and negotiating API versions.

```
443845 records sent, 88769,0 records/sec (86,69 MB/sec), 0,4 ms avg latency, 142,0 ms max latency.
487991 records sent, 97598,2 records/sec (95,31 MB/sec), 0,8 ms avg latency, 61,0 ms max latency.
486854 records sent, 97370,8 records/sec (95,09 MB/sec), 0,3 ms avg latency, 19,0 ms max latency.
489226 records sent, 97845,2 records/sec (95,55 MB/sec), 0,7 ms avg latency, 52,0 ms max latency.
490963 records sent, 98192,6 records/sec (95,89 MB/sec), 0,3 ms avg latency, 19,0 ms max latency.
490906 records sent, 98181,2 records/sec (95,88 MB/sec), 1,3 ms avg latency, 77,0 ms max latency.
480315 records sent, 96063,0 records/sec (93,81 MB/sec), 0,3 ms avg latency, 18,0 ms max latency.
490301 records sent, 98060,2 records/sec (95,76 MB/sec), 1,0 ms avg latency, 61,0 ms max latency.
490337 records sent, 98067,4 records/sec (95,77 MB/sec), 0,2 ms avg latency, 17,0 ms max latency.
486247 records sent, 97249,4 records/sec (94,97 MB/sec), 0,8 ms avg latency, 62,0 ms max latency.
485708 records sent, 97141,6 records/sec (94,86 MB/sec), 0,3 ms avg latency, 18,0 ms max latency.
490806 records sent, 98161,2 records/sec (95,86 MB/sec), 1,7 ms avg latency, 77,0 ms max latency.
478836 records sent, 95767,2 records/sec (93,52 MB/sec), 0,3 ms avg latency, 18,0 ms max latency.
490790 records sent, 98158,0 records/sec (95,86 MB/sec), 0,3 ms avg latency, 18,0 ms max latency.
483014 records sent, 96602,8 records/sec (94,34 MB/sec), 0,7 ms avg latency, 54,0 ms max latency.
490089 records sent, 98017,8 records/sec (95,72 MB/sec), 0,3 ms avg latency, 19,0 ms max latency.
488382 records sent, 97676,4 records/sec (95,39 MB/sec), 1,0 ms avg latency, 74,0 ms max latency.
490937 records sent, 98187,4 records/sec (95,89 MB/sec), 0,3 ms avg latency, 22,0 ms max latency.
482839 records sent, 96567,8 records/sec (94,30 MB/sec), 0,7 ms avg latency, 52,0 ms max latency.
490512 records sent, 98102,4 records/sec (95,80 MB/sec), 0,3 ms avg latency, 18,0 ms max latency.
10000000 records sent, 97093,034546 records/sec (94,82 MB/sec), 0,61 ms avg latency, 142,00 ms max latency, 0 ms 50th, 1 ms 95th, 14 ms 99th, 58 ms 99.9th.
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
446260 records sent, 89252,0 records/sec (87,16 MB/sec), 9,5 ms avg latency, 156,0 ms max latency.
489770 records sent, 97954,0 records/sec (95,66 MB/sec), 0,4 ms avg latency, 18,0 ms max latency.
493483 records sent, 98696,6 records/sec (96,38 MB/sec), 1,8 ms avg latency, 77,0 ms max latency.
494918 records sent, 98983,6 records/sec (96,66 MB/sec), 0,3 ms avg latency, 18,0 ms max latency.
494935 records sent, 98987,0 records/sec (96,67 MB/sec), 1,5 ms avg latency, 60,0 ms max latency.
495877 records sent, 99175,4 records/sec (96,85 MB/sec), 0,7 ms avg latency, 34,0 ms max latency.
490801 records sent, 98160,2 records/sec (95,86 MB/sec), 2,7 ms avg latency, 55,0 ms max latency.
484580 records sent, 96916,0 records/sec (94,64 MB/sec), 1,0 ms avg latency, 39,0 ms max latency.
478055 records sent, 95611,0 records/sec (93,37 MB/sec), 1,6 ms avg latency, 57,0 ms max latency.
493678 records sent, 98735,6 records/sec (96,42 MB/sec), 0,4 ms avg latency, 19,0 ms max latency.
486569 records sent, 97313,8 records/sec (95,03 MB/sec), 1,3 ms avg latency, 54,0 ms max latency.
497780 records sent, 99556,0 records/sec (97,22 MB/sec), 0,4 ms avg latency, 18,0 ms max latency.
492807 records sent, 98561,4 records/sec (96,25 MB/sec), 0,5 ms avg latency, 20,0 ms max latency.
491918 records sent, 98383,6 records/sec (96,08 MB/sec), 1,9 ms avg latency, 73,0 ms max latency.
494801 records sent, 98960,2 records/sec (96,64 MB/sec), 0,4 ms avg latency, 19,0 ms max latency.
496531 records sent, 99306,2 records/sec (96,98 MB/sec), 1,1 ms avg latency, 54,0 ms max latency.
495687 records sent, 99137,4 records/sec (96,81 MB/sec), 0,4 ms avg latency, 18,0 ms max latency.
495224 records sent, 99044,8 records/sec (96,72 MB/sec), 1,3 ms avg latency, 57,0 ms max latency.
495995 records sent, 99199,0 records/sec (96,87 MB/sec), 0,6 ms avg latency, 21,0 ms max latency.
491785 records sent, 98357,0 records/sec (96,05 MB/sec), 1,2 ms avg latency, 54,0 ms max latency.
10000000 records sent, 98015,192355 records/sec (95,72 MB/sec), 1,39 ms avg latency, 156,00 ms max latency, 0 ms 50th, 3 ms 95th, 40 ms 99th, 89 ms 99.9th.
```
