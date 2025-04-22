#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail
PERF_TESTS_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

TEST=${TEST:-'[0-9][0-9]-.*'}
RECORD_SIZE=${RECORD_SIZE:-1024}
NUM_RECORDS=${NUM_RECORDS:-10000000}
PRODUCER_PROPERTIES=${PRODUCER_PROPERTIES:-"acks=all"}
WARM_UP_NUM_RECORDS_POST_BROKER_START=${WARM_UP_NUM_RECORDS_POST_BROKER_START:-1000}
WARM_UP_NUM_RECORDS_PRE_TEST=${WARM_UP_NUM_RECORDS_PRE_TEST:-1000}
COMMIT_ID=${COMMIT_ID:=$(git rev-parse --short HEAD)}


PROFILING_OUTPUT_DIRECTORY=${PROFILING_OUTPUT_DIRECTORY:-"/tmp/results"}

ON_SHUTDOWN=()
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
PURPLE='\033[1;35m'
NOCOLOR='\033[0m'

KROXYLICIOUS_CHECKOUT=${KROXYLICIOUS_CHECKOUT:-${PERF_TESTS_DIR}/..}

DOCKER_REGISTRY="docker.io"
if [ "${USE_DOCKER_MIRROR}" == "true" ]
then
  DOCKER_REGISTRY="mirror.gcr.io"
fi

KAFKA_VERSION=${KAFKA_VERSION:-$(mvn -f "${KROXYLICIOUS_CHECKOUT}"/pom.xml org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=kafka.version -q -DforceStdout -pl kroxylicious-systemtests)}
STRIMZI_VERSION=${STRIMZI_VERSION:-$(mvn -f "${KROXYLICIOUS_CHECKOUT}"/pom.xml org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=strimzi.version -q -DforceStdout)}
KROXYLICIOUS_VERSION=${KROXYLICIOUS_VERSION:-$(mvn -f "${KROXYLICIOUS_CHECKOUT}"/pom.xml org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=project.version -q -DforceStdout)}
KAFKA_TOOL_IMAGE=${KAFKA_TOOL_IMAGE:-quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}}
KAFKA_IMAGE=${KAFKA_IMAGE:-"${DOCKER_REGISTRY}/apache/kafka-native:${KAFKA_VERSION}"}
KROXYLICIOUS_IMAGE=${KROXYLICIOUS_IMAGE:-"quay.io/kroxylicious/kroxylicious:${KROXYLICIOUS_VERSION}"}
VAULT_IMAGE=${VAULT_IMAGE:-"${DOCKER_REGISTRY}/hashicorp/vault:1.19.2"}
PERF_NETWORK=performance-tests_perf_network
CONTAINER_ENGINE=${CONTAINER_ENGINE:-"docker"}
LOADER_DIR=${LOADER_DIR:-"/tmp/asprof-extracted"}
export KAFKA_VERSION KAFKA_TOOL_IMAGE KAFKA_IMAGE KROXYLICIOUS_IMAGE VAULT_IMAGE CONTAINER_ENGINE

printf "KAFKA_VERSION: ${KAFKA_VERSION}\n"
printf "STRIMZI_VERSION: ${STRIMZI_VERSION}\n"
printf "KROXYLICIOUS_VERSION: ${KROXYLICIOUS_VERSION}\n"
printf "KAFKA_IMAGE: ${KAFKA_IMAGE}\n"
printf "KROXYLICIOUS_IMAGE: ${KROXYLICIOUS_IMAGE}\n"
printf "VAULT_IMAGE: ${VAULT_IMAGE}\n"


runDockerCompose () {
  #  Docker compose can't see $UID so need to set here before calling it
  D_UID="$(id -u)" D_GID="$(id -g)" ${CONTAINER_ENGINE} compose -f "${PERF_TESTS_DIR}"/docker-compose.yaml "${@}"
}

doCreateTopic () {
  local TOPIC
  ENDPOINT=$1
  TOPIC=$2
  ${CONTAINER_ENGINE} run --rm --network ${PERF_NETWORK} "${KAFKA_TOOL_IMAGE}" \
      bin/kafka-topics.sh --create --topic "${TOPIC}" --bootstrap-server "${ENDPOINT}" 1>/dev/null
}

doDeleteTopic () {
  local ENDPOINT
  local TOPIC
  ENDPOINT=$1
  TOPIC=$2

  ${CONTAINER_ENGINE} run --rm --network ${PERF_NETWORK} "${KAFKA_TOOL_IMAGE}"  \
      bin/kafka-topics.sh --delete --topic "${TOPIC}" --bootstrap-server "${ENDPOINT}"
}

warmUp() {
  echo -e "${YELLOW}Running warm up${NOCOLOR}"
  producerPerf "$1" "$2" "${WARM_UP_NUM_RECORDS_PRE_TEST}" /dev/null > /dev/null
  consumerPerf "$1" "$2" "${WARM_UP_NUM_RECORDS_PRE_TEST}" /dev/null > /dev/null
}


ensureSysCtlValue() {
  local keyName=$1
  local desiredValue=$2

  if [ "$(sysctl -n "${keyName}")" != "${desiredValue}" ] ;
  then
    echo -e "${YELLOW}setting ${keyName} to ${desiredValue}${NOCOLOR}"
    sudo sysctl "${keyName}"="${desiredValue}"
  fi
}

setupAsyncProfilerKroxy() {
  ensureSysCtlValue kernel.perf_event_paranoid 1
  ensureSysCtlValue kernel.kptr_restrict 0

  mkdir -p /tmp/asprof
  curl -s -o /tmp/asprof/ap-loader-all.jar "https://repo1.maven.org/maven2/me/bechberger/ap-loader-all/3.0-9/ap-loader-all-3.0-9.jar"

  mkdir -p "${LOADER_DIR}"
  unzip -o -q /tmp/asprof/ap-loader-all.jar -d "${LOADER_DIR}"
}

deleteAsyncProfilerKroxy() {
  rm -rf /tmp/asprof
  rm -rf "${LOADER_DIR}"
}

startAsyncProfilerKroxy() {

  echo -e "${PURPLE}Starting async profiler${NOCOLOR}"

  local TARGETARCH=""
  case $(uname -m) in
      aarch64)  TARGETARCH="linux-arm64" ;;
      x86_64 | i686 | i386)   TARGETARCH="linux-x64" ;;
      *)        echo -n "Unsupported arch"
  esac

  echo "TARGETARCH: ${TARGETARCH}"

  ${CONTAINER_ENGINE} exec ${KROXYLICIOUS_CONTAINER_ID} mkdir -p "${LOADER_DIR}/"""{bin,lib} && chmod +r -R "${LOADER_DIR}"
  ${CONTAINER_ENGINE} cp "${LOADER_DIR}/libs/libasyncProfiler-3.0-${TARGETARCH}.so" "${KROXYLICIOUS_CONTAINER_ID}:${LOADER_DIR}/lib/libasyncProfiler.so"

  java -Dap_loader_extraction_dir=${LOADER_DIR} -jar /tmp/asprof/ap-loader-all.jar profiler start "${KROXYLICIOUS_PID}"
}

stopAsyncProfilerKroxy() {
  java -Dap_loader_extraction_dir=${LOADER_DIR} -jar /tmp/asprof/ap-loader-all.jar profiler status "${KROXYLICIOUS_PID}"

  echo -e "${PURPLE}Stopping async profiler${NOCOLOR}"

  ${CONTAINER_ENGINE} exec "${KROXYLICIOUS_CONTAINER_ID}" mkdir -p /tmp/asprof-results
  java -Dap_loader_extraction_dir=${LOADER_DIR} -jar /tmp/asprof/ap-loader-all.jar profiler stop "${KROXYLICIOUS_PID}" -o flamegraph -f "/tmp/asprof-results/${TESTNAME}-cpu-%t.html"

  mkdir -p "${PROFILING_OUTPUT_DIRECTORY}"
  ${CONTAINER_ENGINE} cp "${KROXYLICIOUS_CONTAINER_ID}":/tmp/asprof-results/. "${PROFILING_OUTPUT_DIRECTORY}"
}

# runs kafka-producer-perf-test.sh transforming the output to an array of objects
producerPerf() {
  local ENDPOINT
  local TOPIC
  local NUM_RECORDS
  local OUTPUT
  ENDPOINT=$1
  TOPIC=$2
  NUM_RECORDS=$3
  OUTPUT=$4

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

  # shellcheck disable=SC2086
  ${CONTAINER_ENGINE} run --rm --network ${PERF_NETWORK} "${KAFKA_TOOL_IMAGE}" \
      bin/kafka-producer-perf-test.sh --topic "${TOPIC}" --throughput -1 --num-records "${NUM_RECORDS}" --record-size "${RECORD_SIZE}" \
      --producer-props ${PRODUCER_PROPERTIES} bootstrap.servers="${ENDPOINT}" | \
      jq --raw-input --arg name "${TESTNAME}" --arg commit_id "${COMMIT_ID}" '[.,inputs] | [.[] | match("^(?<sent>\\d+) *records sent" +
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
                                 {name: $name, commit_id: $commit_id, values: [.[] | .captures | map( { (.name|tostring): ( .string | tonumber? ) } ) | add | del(..|nulls)]}' > "${OUTPUT}"
}

consumerPerf() {
  local ENDPOINT
  local TOPIC
  local NUM_RECORDS
  local OUTPUT

  ENDPOINT=$1
  TOPIC=$2
  NUM_RECORDS=$3
  OUTPUT=$4

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

  ${CONTAINER_ENGINE} run --rm --network ${PERF_NETWORK} "${KAFKA_TOOL_IMAGE}"  \
      bin/kafka-consumer-perf-test.sh --topic "${TOPIC}" --messages "${NUM_RECORDS}" --hide-header \
      --bootstrap-server "${ENDPOINT}" |
       jq --raw-input --arg name "${TESTNAME}" --arg commit_id "${COMMIT_ID}"  '[.,inputs] | [.[] | match("^(?<start.time>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}:\\d{3}), " +
                                        "(?<end.time>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}:\\d{3}), " +
                                        "(?<data.consumed.in.MB>\\d+[.]?\\d*), " +
                                        "(?<MB.sec>\\d+[.]?\\d*), " +
                                        "(?<data.consumed.in.nMsg>\\d+[.]?\\d*), " +
                                        "(?<nMsg.sec>\\d+[.]?\\d*), " +
                                        "(?<rebalance.time.ms>\\d+[.]?\\d*), " +
                                        "(?<fetch.time.ms>\\d+[.]?\\d*), " +
                                        "(?<fetch.MB.sec>\\d+[.]?\\d*), " +
                                        "(?<fetch.nMsg.sec>\\d+[.]?\\d*)"; "g")] |
                                 { name: $name, commit_id: $commit_id, values: [.[] | .captures | map( { (.name|tostring): ( .string | tonumber? ) } ) | add | del(..|nulls)]}' > "${OUTPUT}"
}

# expects TEST_NAME, TOPIC, ENDPOINT, PRODUCER_RESULT and CONSUMER_RESULT to be set
doPerfTest () {

  doCreateTopic "${ENDPOINT}" "${TOPIC}"
  warmUp "${ENDPOINT}" "${TOPIC}"

  if [ ! -z "$KROXYLICIOUS_CONTAINER_ID" ] && [ "$(uname)" != 'Darwin' ]
  then
    echo -e "${PURPLE}KROXYLICIOUS_CONTAINER_ID set to $KROXYLICIOUS_CONTAINER_ID${NOCOLOR}"
    startAsyncProfilerKroxy
  fi


  producerPerf "${ENDPOINT}" "${TOPIC}" "${NUM_RECORDS}" "${PRODUCER_RESULT}"

  if [ ! -z "$KROXYLICIOUS_CONTAINER_ID" ] && [ "$(uname)" != 'Darwin' ]
  then
    stopAsyncProfilerKroxy
  fi

  consumerPerf "${ENDPOINT}" "${TOPIC}" "${NUM_RECORDS}" "${CONSUMER_RESULT}"

  doDeleteTopic "${ENDPOINT}" "${TOPIC}"
}

onExit() {
  for cmd in "${ON_SHUTDOWN[@]}"
  do
    eval "${cmd}"
  done
}

trap onExit EXIT

TMP=$(mktemp -d)
ON_SHUTDOWN+=("rm -rf ${TMP}")

# Bring up Kafka
ON_SHUTDOWN+=("runDockerCompose down")

[[ -n ${PULL_CONTAINERS} ]] && runDockerCompose pull

if [ "$(uname)" != 'Darwin' ]; then
  # Async profiler not supported on macOS
  setupAsyncProfilerKroxy
fi
ON_SHUTDOWN+=("deleteAsyncProfilerKroxy")

runDockerCompose up --detach --wait kafka

# Warm up the broker - we do this separately as we might want a longer warm-up period
doCreateTopic broker1:9092 warmup-topic
warmUp broker1:9092 warmup-topic "${WARM_UP_NUM_RECORDS_POST_BROKER_START}"
doDeleteTopic broker1:9092 warmup-topic

echo -e "${GREEN}Running test cases, number of records = ${NUM_RECORDS}, record size ${RECORD_SIZE}${NOCOLOR}"

PRODUCER_RESULTS=()
CONSUMER_RESULTS=()
for t in $(find "${PERF_TESTS_DIR}" -type d -regex '.*/'"${TEST}" | sort)
do
  TESTNAME=$(basename "$t")
  TEST_TMP=${TMP}/${TESTNAME}
  mkdir -p "${TEST_TMP}"
  PRODUCER_RESULT=${TEST_TMP}/producer.json
  CONSUMER_RESULT=${TEST_TMP}/consumer.json
  TOPIC=perf-test-${RANDOM}

  echo -e "${GREEN}Running ${TESTNAME} ${NOCOLOR}"

  TESTNAME=${TESTNAME} TOPIC=${TOPIC} PRODUCER_RESULT=${PRODUCER_RESULT} CONSUMER_RESULT=${CONSUMER_RESULT} . "${t}"/run.sh

  PRODUCER_RESULTS+=("${PRODUCER_RESULT}")
  CONSUMER_RESULTS+=("${CONSUMER_RESULT}")
done

# Summarise results

echo -e "${GREEN}Producer Results for ${COMMIT_ID}${NOCOLOR}"

jq -r -s '(["Name","Sent","Rate rec/s", "Rate Mi/s", "Avg Lat ms", "Max Lat ms", "Percentile50", "Percentile95", "Percentile99", "Percentile999"] | (., map(length*"-"))),
           (.[] | [ .name, (.values | last | .[]) ]) | @tsv' "${PRODUCER_RESULTS[@]}" | column -t -s $'\t'

echo -e "${GREEN}Consumer Results for ${COMMIT_ID}${NOCOLOR}"

jq -r -s '(["Name","Consumed Mi","Consumed Mi/s", "Consumed recs", "Consumed rec/s", "Rebalance Time ms", "Fetch Time ms", "Fetch Mi/s", "Fetch rec/s"] | (., map(length*"-"))),
           (.[] | [ .name, (.values  | last | .[]) ]) | @tsv' "${CONSUMER_RESULTS[@]}" | column -t -s $'\t'


# Write output for integration with Kibana in the CI pipeline
# This maintains the existing interface
if [[ -n "${KIBANA_OUTPUT_DIR}" && -d "${KIBANA_OUTPUT_DIR}" ]]; then
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
