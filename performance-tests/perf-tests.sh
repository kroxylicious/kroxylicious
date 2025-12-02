#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail
PERF_TESTS_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${PERF_TESTS_DIR}/common-perf.sh"

TEST=${TEST:-'[0-9][0-9]-.*'}
RECORD_SIZE=${RECORD_SIZE:-1024}
NUM_RECORDS=${NUM_RECORDS:-10000000}
PRODUCER_PROPERTIES=${PRODUCER_PROPERTIES:-"acks=all"}
WARM_UP_NUM_RECORDS_POST_BROKER_START=${WARM_UP_NUM_RECORDS_POST_BROKER_START:-1000}
WARM_UP_NUM_RECORDS_PRE_TEST=${WARM_UP_NUM_RECORDS_PRE_TEST:-1000}
COMMIT_ID=${COMMIT_ID:=$(git rev-parse --short HEAD)}
KROXYLICIOUS_CHECKOUT=${KROXYLICIOUS_CHECKOUT:-${PERF_TESTS_DIR}/..}
PROFILING_OUTPUT_DIRECTORY=${PROFILING_OUTPUT_DIRECTORY:-"/tmp/profiler-results/"}

export RECORD_SIZE NUM_RECORDS PRODUCER_PROPERTIES WARM_UP_NUM_RECORDS_POST_BROKER_START WARM_UP_NUM_RECORDS_PRE_TEST COMMIT_ID KROXYLICIOUS_CHECKOUT
ON_SHUTDOWN=()


DOCKER_REGISTRY="docker.io"
if [ "${USE_DOCKER_MIRROR:-}" == "true" ]
then
  DOCKER_REGISTRY="mirror.gcr.io"
fi

KAFKA_VERSION=${KAFKA_VERSION:-$(mvn -f "${KROXYLICIOUS_CHECKOUT}"/pom.xml org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=kafka.version -q -DforceStdout -pl kroxylicious-systemtests)}
STRIMZI_VERSION=${STRIMZI_VERSION:-$(mvn -f "${KROXYLICIOUS_CHECKOUT}"/pom.xml org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=strimzi.version -q -DforceStdout)}
KROXYLICIOUS_VERSION=${KROXYLICIOUS_VERSION:-$(mvn -f "${KROXYLICIOUS_CHECKOUT}"/pom.xml org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=project.version -q -DforceStdout)}
KAFKA_TOOL_IMAGE=${KAFKA_TOOL_IMAGE:-quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}}
KAFKA_IMAGE=${KAFKA_IMAGE:-"${DOCKER_REGISTRY}/apache/kafka-native:${KAFKA_VERSION}"}
KROXYLICIOUS_IMAGE=${KROXYLICIOUS_IMAGE:-"quay.io/kroxylicious/kroxylicious:${KROXYLICIOUS_VERSION}"}
VAULT_IMAGE=${VAULT_IMAGE:-"${DOCKER_REGISTRY}/hashicorp/vault:1.21.1"}
PERF_NETWORK=performance-tests_perf_network
CONTAINER_ENGINE=${CONTAINER_ENGINE:-"docker"}
TEST_NAME="warmup"
export KAFKA_VERSION KAFKA_TOOL_IMAGE KAFKA_IMAGE KROXYLICIOUS_IMAGE VAULT_IMAGE CONTAINER_ENGINE TEST_NAME PERF_NETWORK PERF_TESTS_DIR

printf "KAFKA_VERSION=${KAFKA_VERSION}\n"
printf "STRIMZI_VERSION=${STRIMZI_VERSION}\n"
printf "KROXYLICIOUS_VERSION=${KROXYLICIOUS_VERSION}\n"
printf "KAFKA_IMAGE=${KAFKA_IMAGE}\n"
printf "KROXYLICIOUS_IMAGE=${KROXYLICIOUS_IMAGE}\n"
printf "VAULT_IMAGE=${VAULT_IMAGE}\n"

ensureSysCtlValue() {
  local keyName=$1
  local desiredValue=$2

  if [ "$(sysctl -n "${keyName}")" != "${desiredValue}" ] ;
  then
    echo -e "${YELLOW}setting ${keyName} to ${desiredValue}${NO_COLOR}"
    sudo sysctl "${keyName}"="${desiredValue}"
  fi
}

setupAsyncProfilerKroxy() {
  ensureSysCtlValue kernel.perf_event_paranoid 1
  ensureSysCtlValue kernel.kptr_restrict 0
}

onExit() {
  for cmd in "${ON_SHUTDOWN[@]}"
  do
    eval "${cmd}"
  done
}

mapfile -t TESTCASES -d " " < <( find "${PERF_TESTS_DIR}" -type d -regex '.*/'"${TEST}" | sort )
if [ ${#TESTCASES[@]} -eq 0 ]; then
  echo -e "${RED}No test cases matched: find \"${PERF_TESTS_DIR}\" -type d -regex '.*/'\"${TEST}\" exiting ${NO_COLOR}" >&2
  exit 1
else
  echo -e "${GREEN}Found tests: ${NO_COLOR}"
  for test_path in "${TESTCASES[@]}"; do
    echo "${test_path}"
  done
fi

trap onExit EXIT

TMP=$(mktemp -d)
ON_SHUTDOWN+=("rm -rf ${TMP}")

# Bring up Kafka
#ON_SHUTDOWN+=("runDockerCompose down")

[[ -n ${PULL_CONTAINERS:-} ]] && runDockerCompose pull

if [ "$(uname)" != 'Darwin' ]; then
  # Kernel/perf-events profiler not supported on macOS
  setupAsyncProfilerKroxy
fi

runDockerCompose up --detach --wait kafka

# Warm up the broker - we do this separately as we might want a longer warm-up period
doCreateTopic broker1:9092 warmup-topic
warmUp broker1:9092 warmup-topic "${WARM_UP_NUM_RECORDS_POST_BROKER_START}"
doDeleteTopic broker1:9092 warmup-topic

echo -e "${GREEN}Running test cases, number of records = ${NUM_RECORDS}, record size ${RECORD_SIZE}${NO_COLOR}"

PRODUCER_RESULTS=()
CONSUMER_RESULTS=()

for t in "${TESTCASES[@]}"
do
  TEST_NAME=$(basename "$t")
  export TEST_NAME
  TEST_TMP=${TMP}/${TEST_NAME}
  mkdir -p "${TEST_TMP}"
  PRODUCER_RESULT=${TEST_TMP}/producer.json
  CONSUMER_RESULT=${TEST_TMP}/consumer.json
  TOPIC=perf-test-${RANDOM}
  mkdir -p "${PROFILING_OUTPUT_DIRECTORY}/${TEST_NAME}" && chmod a+w "${PROFILING_OUTPUT_DIRECTORY}/${TEST_NAME}"

  echo -e "${GREEN}Running ${TEST_NAME} ${NO_COLOR}"
  TOPIC=${TOPIC} PRODUCER_RESULT=${PRODUCER_RESULT} CONSUMER_RESULT=${CONSUMER_RESULT} "${t}/run.sh"

  PRODUCER_RESULTS+=("${PRODUCER_RESULT}")
  CONSUMER_RESULTS+=("${CONSUMER_RESULT}")
done

# Summarise results

if [ ${#PRODUCER_RESULTS[@]} -ne 0 ]; then
  echo -e "${GREEN}Producer Results for ${COMMIT_ID}${NO_COLOR}"
  jq -r -s '(["Name","Sent","Rate rec/s", "Rate Mi/s", "Avg Lat ms", "Max Lat ms", "Percentile50", "Percentile95", "Percentile99", "Percentile999"] | (., map(length*"-"))),
           (.[] | [ .name, (.values | last | .[]) ]) | @tsv' "${PRODUCER_RESULTS[@]}" | column -t -s $'\t'
else
  echo -e "${YELLOW}NO Producer Results for ${COMMIT_ID}${NO_COLOR}"
fi

if [ ${#CONSUMER_RESULTS[@]} -ne 0 ]; then
echo -e "${GREEN}Consumer Results for ${COMMIT_ID}${NO_COLOR}"

jq -r -s '(["Name","Consumed Mi","Consumed Mi/s", "Consumed recs", "Consumed rec/s", "Rebalance Time ms", "Fetch Time ms", "Fetch Mi/s", "Fetch rec/s"] | (., map(length*"-"))),
           (.[] | [ .name, (.values  | last | .[]) ]) | @tsv' "${CONSUMER_RESULTS[@]}" | column -t -s $'\t'
else
  echo -e "${YELLOW}NO Consumer Results for ${COMMIT_ID}${NO_COLOR}"
fi

# Write output for integration with Kibana in the CI pipeline
# This maintains the existing interface
if [[ -n "${KIBANA_OUTPUT_DIR:-}" && -d "${KIBANA_OUTPUT_DIR:-}" ]]; then
  for PRODUCER_RESULT in "${PRODUCER_RESULTS[@]}"
  do
    DIR=${KIBANA_OUTPUT_DIR}/$(jq -r '.name' "${PRODUCER_RESULT}")
    mkdir -p "${DIR}"
    jq '.values | last | [{name: "Rate rps", unit: "rec/s", value: .rate_rps},
                          {name: "Rate mips", unit: "Mi/s", value: .rate_mips},
                          {name: "AVG Latency", unit: "ms", value: .avg_lat_ms},
                          {name: "95th Latency", unit: "ms", value: .percentile95},
                          {name: "99th Latency", unit: "ms", value: .percentile99}]' "${PRODUCER_RESULT}" > "${DIR}"/producer.json
  done

  for CONSUMER_RESULT in "${CONSUMER_RESULTS[@]}"
  do
    DIR=${KIBANA_OUTPUT_DIR}/$(jq -r '.name' "${CONSUMER_RESULT}")
    jq '[.values | last | to_entries[] | { name: .key, value} ]' "${CONSUMER_RESULT}" > "${DIR}"/consumer.json
  done
fi
