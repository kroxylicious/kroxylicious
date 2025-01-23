# Performance

This document describes how to run some basic performance tests for the proxy.

## Overview

There is a simple performance test framework that uses Apache Kafka's `kafka-producer-perf-test.sh`
and `kafka-consumer-perf-test.sh` to get simple throughput and latency numbers for Kroxylicious use-cases.

The script uses Docker Compose to orchestrate the tests with Apache Kafka, Kroxylicious and HashiCorp Vault
running within containers.

Five scenarios are tested, including a baseline for Apache Kafka alone.  That lets you understand the cost of
adding Kroxylicious in terms of throughput and latency.    The tests currently use a Kafka Cluster with a single
broker.

The tested scenarios are:

* Baseline (Kafka only)
* Kroxylicious + Kafka - no filters
* Kroxylicious + Kafka - simple transform filter
* Kroxylicious + Kafka - envelope encryption with encrypted topic
* Kroxylicious + Kafka - envelope encryption without encrypted topic

## Prerequisites 

- [JDK](https://openjdk.org/projects/jdk/17/) (version 17 and above) - JDK
- [`mvn`](https://maven.apache.org/index.html) (version 3.5 and above) - Maven CLI
- [`docker`](https://docs.docker.com/install/) or [`podman`](https://podman.io/docs/installation) - Docker or Podman
- [`jq`](https://jqlang.github.io/) 1.7 or higher

## Running

The performance tests are invoked using a script

```shell
./performance-tests/perf-tests.sh
```

The script will produce output like this:

```shell
Producer Results
Name                                      Sent      Rate rec/s    Rate Mi/s  Avg Lat ms  Max Lat ms  Percentile50  Percentile95  Percentile99  Percentile999
----                                      ----      ----------    ---------  ----------  ----------  ------------  ------------  ------------  -------------
01-no-proxy-baseline                      10000000  48671.274214  47.53      628.71      1354.00     625           669           692           1326
02-no-filters                             10000000  47188.954010  46.08      648.50      1630.00     637           704           873           1601
03-transform-filter                       10000000  47623.810000  46.51      642.44      882.00      639           686           716           818
04-record-encryption-filter               10000000  45558.086560  44.49      671.66      1840.00     662           720           814           1621
05-record-encryption-filter-no-encrypt    10000000  13195.153684  12.89      2323.05     4244.00     2304          2385          2867          3807
Consumer Results
Name                                      Consumed Mi  Consumed Mi/s  Consumed recs  Consumed rec/s  Rebalance Time ms  Fetch Time ms  Fetch Mi/s  Fetch rec/s
----                                      -----------  -------------  -------------  --------------  -----------------  -------------  ----------  -----------
01-no-proxy-baseline                      9765.7471    622.2996       10000125       637234.7543     289                15404          633.9748    649190.1454
02-no-filters                             9765.7471    317.1006       10000125       324711.0108     307                30490          320.2934    327980.4854
03-transform-filter                       9765.7471    341.4597       10000125       349654.7203     295                28305          345.0184    353298.8871
04-record-encryption-filter               9765.6445    104.6099       10000020       107120.4996     290                93063          104.9358    107454.3051
05-record-encryption-filter-no-encrypt    9765.7471    142.2749       10000125       145689.4668     295                68345          142.8890    146318.3115
```

## Configuration

The script understands the following environment variables.

| Environment Variable                   | Description                                                                                  |
|----------------------------------------|----------------------------------------------------------------------------------------------|
| TEST                                   | Test(s) to run. This is a regular expression that matches the test directories.              |
| NUM_RECORDS                            | Number of records produced/consumed by the test, defaults to 10000000                        |
| WARM_UP_NUM_RECORDS_POST_BROKER_START  | Warm up records sent after the broker is started, before the first test.                     |
|                                        | The purpose of this is to ensure that the broker is warm.                                    |
| WARM_UP_NUM_RECORDS_PRE_TEST           | Warm up records sent before each test.                                                       |
|                                        | The purpose of this is to ensure that the proxy is warm.                                     |
| RECORD_SIZE                            | Record size in bytes, defaults to 1024                                                       |
| KROXYLICIOUS_IMAGE                     | Kroxylicious image.  Defaults to the last snapshot image produced from main by CI            |
| KAFKA_IMAGE                            | Kafka native image.  Defaults to use the image that match the `kafka.version' of the project |
| KAFKA_TOOL_IMAGE                       | Kafka tooling image.  Defaults to use Kafka image published by the Strimzi project that      |
|                                        | `strimzi.version` property of the project.                                                   |
| VAULT_IMAGE                            | HashiCorp Vault image.                                                                       |
| USE_DOCKER_MIRROR                      | If `true` uses the gcr mirror to access docker images, otherwise uses docker hub repository  |

# Jenkins pipeline for performance

When a PR is created and the performance tests are needed, if you are a member of
[Developers](https://github.com/orgs/kroxylicious/teams/developers), you may add the following comment into the PR to trigger the run.

```
@strimzi-ci run perf
```

It will launch the `kroxylicious-performance-tests-pr` build, that will insert a comment with a summary into the PR comparing the results with the previous execution.

In case a manual execution needs to be done, this job is ready for that `kroxylicious-performance-tests`, where
some parameters can be configurable (NUMBER_OF_MESSAGES, MESSAGE_SIZE, PRODUCER_PROPERTIES)
