#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
NUM_RECORDS=${NUM_RECORDS:-10000000}
RECORD_SIZE=${RECORD_SIZE:-1024}
ON_SHUTDOWN=()
PERF_TEST=${SCRIPT_DIR}/perf-tests
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NOCOLOR='\033[0m'

. "${SCRIPT_DIR}/common.sh"
KROXYLICIOUS_CHECKOUT=${KROXYLICIOUS_CHECKOUT:-${SCRIPT_DIR}/..}

KAFKA_VERSION=${KAFKA_VERSION:-$(mvn -f ${KROXYLICIOUS_CHECKOUT}/pom.xml org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=kafka.version -q -DforceStdout)}
STRIMZI_VERSION=${STRIMZI_VERSION:-$(mvn -f ${KROXYLICIOUS_CHECKOUT}/pom.xml org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=strimzi.version -q -DforceStdout)}
export KAFKA_VERSION STRIMZI_VERSION

runDockerCompose () {
  docker-compose -f ${PERF_TEST}/docker-compose.yaml "${@}"
}

doCreateTopic () {
  local TOPIC
  ENDPOINT=$1
  TOPIC=$2
  docker run --rm --network perf-tests_perf_network quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}  \
      bin/kafka-topics.sh --create --topic ${TOPIC} --bootstrap-server ${ENDPOINT} 1>/dev/null
}

doDeleteTopic () {
  local ENDPOINT
  local TOPIC
  ENDPOINT=$1
  TOPIC=$2
  docker run --rm --network perf-tests_perf_network quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}  \
      bin/kafka-topics.sh --delete --topic ${TOPIC} --bootstrap-server ${ENDPOINT}
}


warmUp() {
  echo -e "${YELLOW}Running warm up${NOCOLOR}"
  producerPerf $1 $2 1000 > /dev/null
  consumerPerf $1 $2 1000
}

# runs kafka-producer-perf-test.sh transforming the output to an array of objects
producerPerf() {
  local ENDPOINT
  local TOPIC
  local NUM_RECORDS
  ENDPOINT=$1
  TOPIC=$2
  NUM_RECORDS=$3

  echo -e "${YELLOW}Running producer test${NOCOLOR}"

  # Input:
  # 250000 records sent, 41757.140471 records/sec (40.78 MB/sec), 639.48 ms avg latency, 782.00 ms max latency
  # 250000 records sent, 41757.140471 records/sec (40.78 MB/sec), 639.48 ms avg latency, 782.00 ms max latency, 670 ms 50th, 771 ms 95th, 777 ms 99th, 781 ms 99.9th
  # Output:
  # [
  #  { "sent": 204796, "rate_rps": 40959.2, "rate_mips": 40.00, "avg_lat_ms": 627.9, "max_lat_ms": 759.0 },
  #  { "sent": 300000, "rate_rps": 43184.108248, "rate_mips": 42.17, "avg_lat_ms": 627.62, "max_lat_ms": 759.00,
  #    "percentile50": 644, "percentile95": 744, "percentile99": 753, "percentile999": 758 }
  # ]

  docker run --rm --network perf-tests_perf_network quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}  \
      bin/kafka-producer-perf-test.sh --topic ${TOPIC} --throughput -1 --num-records ${NUM_RECORDS} --record-size ${RECORD_SIZE} \
      --producer-props acks=all bootstrap.servers=${ENDPOINT} | \
      jq -R '[.,inputs] | [.[] | match("^(?<sent>\\d+) *records sent" +
                                    ", *(?<rate_rps>\\d+[.]?\\d*) records/sec [(](?<rate_mips>\\d+[.]?\\d*) MB/sec[)]" +
                                    ", *(?<avg_lat_ms>\\d+[.]?\\d*) ms avg latency" +
                                    ", *(?<max_lat_ms>\\d+[.]?\\d*) ms max latency" +
                                    "(?<inflight>" +
                                    ", *(?<percentile50>\\d+[.]?\\d*) ms 50th" +
                                    ", *(?<percentile95>\\d+[.]?\\d*) ms 95th" +
                                    ", *(?<percentile99>\\d+[.]?\\d*) ms 99th" +
                                    ", *(?<percentile999>\\d+[.]?\\d*) ms 99.9th" +
                                    ")?" +
                                    "[.]"; "g")]  |
                                 [.[] | .captures | map( { (.name|tostring): ( .string | tonumber? ) } ) | add | del(..|nulls)]'
}

consumerPerf() {
  local ENDPOINT
  local TOPIC
  local NUM_RECORDS
  ENDPOINT=$1
  TOPIC=$2
  NUM_RECORDS=$3

  echo -e "${YELLOW}Running consumer test${NOCOLOR}"

  # Input:
  # start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
  # 2024-02-21 19:36:23:839, 2024-02-21 19:36:24:256, 0.9766, 2.3419, 1000, 2398.0815, 364, 53, 18.4257, 18867.9245  # Output:
  # Output
  # [
  #  { "sent": 204796, "rate_rps": 40959.2, "rate_mips": 40.00, "avg_lat_ms": 627.9, "max_lat_ms": 759.0 },
  #  { "sent": 300000, "rate_rps": 43184.108248, "rate_mips": 42.17, "avg_lat_ms": 627.62, "max_lat_ms": 759.00,
  #    "percentile50": 644, "percentile95": 744, "percentile99": 753, "percentile999": 758 }
  # ]

  docker run --rm --network perf-tests_perf_network quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}  \
      bin/kafka-consumer-perf-test.sh --topic ${TOPIC} --messages ${NUM_RECORDS} --hide-header \
      --bootstrap-server ${ENDPOINT} |
       jq -R '[.,inputs] | [.[] | match("^(?<start_time>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}:\\d{3}), " +
                                        "(?<end_time>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}:\\d{3}), " +
                                        "(?<consumed_mi>\\d+[.]?\\d*), " +
                                        "(?<consumed_mi_per_sec>\\d+[.]?\\d*), " +
                                        "(?<consumed_rec>\\d+[.]?\\d*), " +
                                        "(?<consumed_rec_per_sec>\\d+[.]?\\d*), " +
                                        "(?<rebalance_time_ms>\\d+[.]?\\d*), " +
                                        "(?<fetch_time_ms>\\d+[.]?\\d*), " +
                                        "(?<fetch_mi_per_sec>\\d+[.]?\\d*), " +
                                        "(?<fetch_rec_per_sec>\\d+[.]?\\d*)"; "g")] |
                                 [.[] | .captures | map( { (.name|tostring): ( .string | tonumber? ) } ) | add | del(..|nulls)]'

#      | \
#      jq -R '[.,inputs] | [.[] | match("^(?<sent>\\d+) *records sent" +
#                                    ", *(?<rate_rps>\\d+[.]?\\d*) records/sec [(](?<rate_mips>\\d+[.]?\\d*) MB/sec[)]" +
#                                    ", *(?<avg_lat_ms>\\d+[.]?\\d*) ms avg latency" +
#                                    ", *(?<max_lat_ms>\\d+[.]?\\d*) ms max latency" +
#                                    "(?<inflight>" +
#                                    ", *(?<percentile50>\\d+[.]?\\d*) ms 50th" +
#                                    ", *(?<percentile95>\\d+[.]?\\d*) ms 95th" +
#                                    ", *(?<percentile99>\\d+[.]?\\d*) ms 99th" +
#                                    ", *(?<percentile999>\\d+[.]?\\d*) ms 99.9th" +
#                                    ")?" +
#                                    "[.]"; "g")]  |
#                                 [.[] | .captures | map( { (.name|tostring): ( .string | tonumber? ) } ) | add | del(..|nulls)]'
}

doPerfKafkaDirect () {
  local TOPIC
  local EP
  TOPIC=perf-test
  EP=broker1:9092

  echo -e "${GREEN}Running Kafka Direct ${NOCOLOR}"
  doCreateTopic ${EP} ${TOPIC}
  warmUp ${EP} ${TOPIC}

  producerPerf ${EP} ${TOPIC} ${NUM_RECORDS}
  consumerPerf ${EP} ${TOPIC} ${NUM_RECORDS}
  doDeleteTopic ${EP} ${TOPIC}
}


doPerfKroxyliciousNoFilters () {
  local TOPIC
  local CFG
  local EP
  TOPIC=perf-test
  CFG=no-filters.yaml
  EP=kroxylicious:9092
  echo -e "${GREEN}Running Kroxylicious No Filter ${NOCOLOR}"

  KROXYLICIOUS_CONFIG=${CFG} runDockerCompose up --detach --wait kroxylicious

  doCreateTopic ${EP} ${TOPIC}

  warmUp ${EP} ${TOPIC}
  producerPerf ${EP} ${TOPIC} ${NUM_RECORDS}

  doDeleteTopic ${EP} ${TOPIC}
  KROXYLICIOUS_CONFIG=${CFG} runDockerCompose rm -s -f kroxylicious
}

doPerfKroxyliciousTransformFilter () {
  local TOPIC
  local CFG
  local EP
  TOPIC=perf-test
  CFG=transform-filter.yaml
  EP=kroxylicious:9092

  echo -e "${GREEN}Running Kroxylicious Transform Filter ${NOCOLOR}"

  KROXYLICIOUS_CONFIG=${CFG} runDockerCompose up --detach --wait kroxylicious
  doCreateTopic ${EP} ${TOPIC}

  warmUp ${EP} ${TOPIC}
  producerPerf ${EP} ${TOPIC} ${NUM_RECORDS}

  doDeleteTopic ${EP} ${TOPIC}
  KROXYLICIOUS_CONFIG=${CFG} runDockerCompose rm -s -f kroxylicious
}

doPerfKroxyliciousEnvelopeEncryptionFilter () {
  local CFG
  local TOPIC
  local EP
  local ENCRYPT
  ENCRYPT=$1
  TOPIC=perf-test
  CFG=envelope-encryption-filter.yaml
  EP=kroxylicious:9092

  echo -e "${GREEN}Running Kroxylicious Envelope Encryption Filter (encrypted topic: ${ENCRYPT}) ${NOCOLOR}"

  KROXYLICIOUS_CONFIG=${CFG} runDockerCompose up --detach --wait kroxylicious vault

  docker exec vault vault secrets enable transit 1>/dev/null
  if [[ ${ENCRYPT} = true ]]; then
    docker exec vault vault write -f transit/keys/KEK_${TOPIC} 1>/dev/null
  fi
  doCreateTopic ${EP} ${TOPIC}

  warmUp ${EP} ${TOPIC}
  producerPerf ${EP} ${TOPIC} ${NUM_RECORDS}

  doDeleteTopic ${EP} ${TOPIC}
  KROXYLICIOUS_CONFIG=${CFG} runDockerCompose rm -s -f kroxylicious vault
}

onExit() {
  for cmd in "${ON_SHUTDOWN[@]}"
  do
    eval ${cmd}
  done
}


trap onExit EXIT

echo -e "${YELLOW}Kafka version is ${KAFKA_VERSION}, Strimzi version ${STRIMZI_VERSION}${NOCOLOR}"

# Bring up Kafka
ON_SHUTDOWN+=("runDockerCompose down")
runDockerCompose up --detach --wait kafka

echo -e "${GREEN}Running test cases, number of records = ${NUM_RECORDS}, record size ${RECORD_SIZE}${NOCOLOR}"

doPerfKafkaDirect
#doPerfKroxyliciousNoFilters
#doPerfKroxyliciousTransformFilter
#doPerfKroxyliciousEnvelopeEncryptionFilter true
#doPerfKroxyliciousEnvelopeEncryptionFilter false

