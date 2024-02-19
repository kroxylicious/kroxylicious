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
cd "${SCRIPT_DIR}/.."

runDockerCompose () {
  docker-compose -f ${PERF_TEST}/docker-compose.yaml "${@}"
}

doCreateTopic () {
  local TOPIC
  ENDPOINT=$1
  TOPIC=$2
  docker run --rm --network perf-tests_perf_network quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}  \
      bin/kafka-topics.sh --create --topic ${TOPIC} --bootstrap-server ${ENDPOINT}
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
  producerPerf $1 $2 1
}

# runs kafka-producer-perf-test.sh transforming the output to an array of objects
producerPerf() {
  local ENDPOINT
  local TOPIC
  local NUM_RECORDS
  ENDPOINT=$1
  TOPIC=$2
  NUM_RECORDS=$3

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

doPerfKafkaDirect () {
  local TOPIC
  TOPIC=perf-test

  echo -e "${GREEN}Running Kafka Direct ${NOCOLOR}"
  echo -e "${YELLOW}Running warm up${NOCOLOR}"
  doCreateTopic broker1:9092 ${TOPIC}
  warmUp broker1:9092 ${TOPIC} > /dev/null

  echo -e "${YELLOW}Running test${NOCOLOR}"
  producerPerf broker1:9092 ${TOPIC} ${NUM_RECORDS}
  doDeleteTopic broker1:9092 perf-test
}


doPerfKroxyNoFilters () {
  local TOPIC
  local CFG
  TOPIC=perf-test
  CFG=no-filters.yaml
  echo -e "${GREEN}Running Kroxylicious No Filter ${NOCOLOR}"

  KROXY_CONFIG=${CFG} runDockerCompose up --detach --wait kroxy

  doCreateTopic kroxy:9092 ${TOPIC}

  echo -e "${YELLOW}Running warm up${NOCOLOR}"
  warmUp kroxy:9092 ${TOPIC} > /dev/null
  echo -e "${YELLOW}Running test${NOCOLOR}"
  producerPerf kroxy:9092 ${TOPIC} ${NUM_RECORDS}

  doDeleteTopic kroxy:9092 ${TOPIC}
  KROXY_CONFIG=${CFG} runDockerCompose rm -s -f kroxy
}

doPerfKroxyTransformFilter () {
  local TOPIC
  local CFG
  TOPIC=perf-test
  CFG=transform-filter.yaml

  echo -e "${GREEN}Running Kroxylicious Transform Filter ${NOCOLOR}"

  KROXY_CONFIG=${CFG} runDockerCompose up --detach --wait kroxy
  doCreateTopic kroxy:9092 ${TOPIC}

  echo -e "${YELLOW}Running warm up${NOCOLOR}"
  warmUp kroxy:9092 ${TOPIC} > /dev/null
  echo -e "${YELLOW}Running test${NOCOLOR}"
  producerPerf kroxy:9092 ${TOPIC} ${NUM_RECORDS}

  doDeleteTopic kroxy:9092 ${TOPIC}
  KROXY_CONFIG=${CFG} runDockerCompose rm -s -f kroxy
}

doPerfKroxyEnvelopeEncryptionFilter () {
  local CFG
  local TOPIC
  local ENCRYPT
  ENCRYPT=$1
  TOPIC=perf-test
  CFG=envelope-encryption-filter.yaml

  echo -e "${GREEN}Running Kroxylicious Envelope Encryption Filter (encrypted topic ${ENCRYPT}) ${NOCOLOR}"

  KROXY_CONFIG=${CFG} runDockerCompose up --detach --wait kroxy vault

  docker exec vault vault secrets enable transit
  if [[ ${ENCRYPT} = true ]]; then
    docker exec vault vault write -f transit/keys/KEK_${TOPIC}
  fi
  doCreateTopic kroxy:9092 ${TOPIC}

  echo -e "${YELLOW}Running warm up${NOCOLOR}"
  warmUp kroxy:9092 ${TOPIC} > /dev/null
  echo -e "${YELLOW}Running test${NOCOLOR}"
  producerPerf kroxy:9092 ${TOPIC} ${NUM_RECORDS}

  doDeleteTopic kroxy:9092 ${TOPIC}
  KROXY_CONFIG=${CFG} runDockerCompose rm -s -f kroxy vault
}

onExit() {
  for cmd in "${ON_SHUTDOWN[@]}"
  do
    eval ${cmd}
  done
}

KAFKA_VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=kafka.version -q -DforceStdout)
STRIMZI_VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=strimzi.version -q -DforceStdout)
export KAFKA_VERSION STRIMZI_VERSION

trap onExit EXIT

echo -e "${YELLOW}Kafka version is ${KAFKA_VERSION}, Strimzi version ${STRIMZI_VERSION}${NOCOLOR}"

# Bring up Kafka
ON_SHUTDOWN+=("KROXY_CONFIG=unused.yaml runDockerCompose down")
KROXY_CONFIG=unused.yaml runDockerCompose up --detach --wait kafka

echo -e "${GREEN}Running test cases, number of records = ${NUM_RECORDS}, record size ${RECORD_SIZE}${NOCOLOR}"

doPerfKafkaDirect
doPerfKroxyNoFilters
doPerfKroxyTransformFilter
doPerfKroxyEnvelopeEncryptionFilter true
doPerfKroxyEnvelopeEncryptionFilter false

