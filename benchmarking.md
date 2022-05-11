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

```
504061 records sent, 100812,2 records/sec (98,45 MB/sec), 3,5 ms avg latency, 323,0 ms max latency.
601707 records sent, 120341,4 records/sec (117,52 MB/sec), 0,5 ms avg latency, 24,0 ms max latency.
599270 records sent, 119854,0 records/sec (117,04 MB/sec), 0,4 ms avg latency, 19,0 ms max latency.
598065 records sent, 119613,0 records/sec (116,81 MB/sec), 0,5 ms avg latency, 31,0 ms max latency.
574570 records sent, 114914,0 records/sec (112,22 MB/sec), 0,5 ms avg latency, 19,0 ms max latency.
560424 records sent, 112084,8 records/sec (109,46 MB/sec), 0,8 ms avg latency, 35,0 ms max latency.
588550 records sent, 117475,0 records/sec (114,72 MB/sec), 0,9 ms avg latency, 37,0 ms max latency.
593033 records sent, 118606,6 records/sec (115,83 MB/sec), 0,9 ms avg latency, 77,0 ms max latency.
586781 records sent, 117356,2 records/sec (114,61 MB/sec), 1,4 ms avg latency, 75,0 ms max latency.
604276 records sent, 120855,2 records/sec (118,02 MB/sec), 0,5 ms avg latency, 36,0 ms max latency.
593010 records sent, 118602,0 records/sec (115,82 MB/sec), 0,6 ms avg latency, 28,0 ms max latency.
591993 records sent, 118398,6 records/sec (115,62 MB/sec), 0,6 ms avg latency, 19,0 ms max latency.
598984 records sent, 119796,8 records/sec (116,99 MB/sec), 0,5 ms avg latency, 22,0 ms max latency.
580779 records sent, 116155,8 records/sec (113,43 MB/sec), 0,6 ms avg latency, 20,0 ms max latency.
586869 records sent, 117373,8 records/sec (114,62 MB/sec), 9,0 ms avg latency, 159,0 ms max latency.
599970 records sent, 119994,0 records/sec (117,18 MB/sec), 2,6 ms avg latency, 86,0 ms max latency.
602751 records sent, 120550,2 records/sec (117,72 MB/sec), 0,4 ms avg latency, 19,0 ms max latency.
10000000 records sent, 117096,018735 records/sec (114,35 MB/sec), 1,39 ms avg latency, 323,00 ms max latency, 0 ms 50th, 3 ms 95th, 34 ms 99th, 105 ms 99.9th.
```

### Proxy - pass-through

```
451250 records sent, 90250,0 records/sec (88,13 MB/sec), 19,9 ms avg latency, 462,0 ms max latency.
538021 records sent, 107604,2 records/sec (105,08 MB/sec), 2,4 ms avg latency, 62,0 ms max latency.
541592 records sent, 108318,4 records/sec (105,78 MB/sec), 0,6 ms avg latency, 19,0 ms max latency.
528529 records sent, 105515,9 records/sec (103,04 MB/sec), 0,9 ms avg latency, 44,0 ms max latency.
575694 records sent, 115138,8 records/sec (112,44 MB/sec), 0,6 ms avg latency, 60,0 ms max latency.
568360 records sent, 113672,0 records/sec (111,01 MB/sec), 0,6 ms avg latency, 43,0 ms max latency.
576083 records sent, 115216,6 records/sec (112,52 MB/sec), 0,7 ms avg latency, 32,0 ms max latency.
574729 records sent, 114945,8 records/sec (112,25 MB/sec), 0,4 ms avg latency, 19,0 ms max latency.
579494 records sent, 115898,8 records/sec (113,18 MB/sec), 0,4 ms avg latency, 19,0 ms max latency.
575604 records sent, 115120,8 records/sec (112,42 MB/sec), 0,5 ms avg latency, 24,0 ms max latency.
578901 records sent, 115780,2 records/sec (113,07 MB/sec), 0,4 ms avg latency, 19,0 ms max latency.
575414 records sent, 115082,8 records/sec (112,39 MB/sec), 0,4 ms avg latency, 19,0 ms max latency.
559432 records sent, 111886,4 records/sec (109,26 MB/sec), 0,8 ms avg latency, 44,0 ms max latency.
567223 records sent, 113444,6 records/sec (110,79 MB/sec), 0,4 ms avg latency, 18,0 ms max latency.
576326 records sent, 115265,2 records/sec (112,56 MB/sec), 0,6 ms avg latency, 36,0 ms max latency.
568820 records sent, 113764,0 records/sec (111,10 MB/sec), 0,5 ms avg latency, 19,0 ms max latency.
576372 records sent, 115274,4 records/sec (112,57 MB/sec), 0,4 ms avg latency, 27,0 ms max latency.
10000000 records sent, 111830,554344 records/sec (109,21 MB/sec), 1,51 ms avg latency, 462,00 ms max latency, 0 ms 50th, 3 ms 95th, 39 ms 99th, 99 ms 99.9th.
```

### Proxy - Lower-case transform

```
457064 records sent, 91412,8 records/sec (89,27 MB/sec), 106,1 ms avg latency, 400,0 ms max latency.
601476 records sent, 120295,2 records/sec (117,48 MB/sec), 8,3 ms avg latency, 85,0 ms max latency.
610251 records sent, 122050,2 records/sec (119,19 MB/sec), 4,2 ms avg latency, 32,0 ms max latency.
613422 records sent, 122684,4 records/sec (119,81 MB/sec), 4,3 ms avg latency, 44,0 ms max latency.
608081 records sent, 121616,2 records/sec (118,77 MB/sec), 5,0 ms avg latency, 52,0 ms max latency.
612203 records sent, 122416,1 records/sec (119,55 MB/sec), 4,0 ms avg latency, 33,0 ms max latency.
612248 records sent, 122425,1 records/sec (119,56 MB/sec), 3,8 ms avg latency, 31,0 ms max latency.
605804 records sent, 121160,8 records/sec (118,32 MB/sec), 5,0 ms avg latency, 36,0 ms max latency.
612977 records sent, 122595,4 records/sec (119,72 MB/sec), 4,1 ms avg latency, 33,0 ms max latency.
611914 records sent, 122382,8 records/sec (119,51 MB/sec), 3,9 ms avg latency, 29,0 ms max latency.
607661 records sent, 121532,2 records/sec (118,68 MB/sec), 10,4 ms avg latency, 117,0 ms max latency.
611379 records sent, 122275,8 records/sec (119,41 MB/sec), 6,2 ms avg latency, 62,0 ms max latency.
611282 records sent, 122256,4 records/sec (119,39 MB/sec), 3,8 ms avg latency, 32,0 ms max latency.
609150 records sent, 121830,0 records/sec (118,97 MB/sec), 4,7 ms avg latency, 44,0 ms max latency.
616676 records sent, 123335,2 records/sec (120,44 MB/sec), 3,7 ms avg latency, 29,0 ms max latency.
603943 records sent, 120764,4 records/sec (117,93 MB/sec), 4,3 ms avg latency, 34,0 ms max latency.
10000000 records sent, 119918,455450 records/sec (117,11 MB/sec), 9,76 ms avg latency, 400,00 ms max latency, 3 ms 50th, 27 ms 95th, 184 ms 99th, 293 ms 99.9th.
```
