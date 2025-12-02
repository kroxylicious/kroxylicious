#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -Eu
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
PURPLE='\033[1;35m'
BLUE='\033[0;34m'
NO_COLOR='\033[0m'



export GREEN RED YELLOW PURPLE BLUE NO_COLOR

runDockerCompose () {
  #  Docker compose can't see $UID so need to set here before calling it
  ${CONTAINER_ENGINE} compose -f "${PERF_TESTS_DIR}/docker-compose.yaml" "${@}"
}

setupProxyConfig() {
  local kroxylicious_config=${1}
  cp "${kroxylicious_config}" "${PERF_TESTS_DIR}/proxy-config.yaml"
}

doCreateTopic () {
  local TOPIC
  ENDPOINT=$1
  TOPIC=$2
  echo "creating topic ${TOPIC} using ${ENDPOINT}"
  ${CONTAINER_ENGINE} run --rm --network "${PERF_NETWORK}" "${KAFKA_TOOL_IMAGE}" \
      bin/kafka-topics.sh --create --if-not-exists  --topic "${TOPIC}" --bootstrap-server "${ENDPOINT}" 1>/dev/null
}

doDeleteTopic () {
  local ENDPOINT
  local TOPIC
  ENDPOINT=$1
  TOPIC=$2
  echo "deleting topic ${TOPIC} using ${ENDPOINT}"
  ${CONTAINER_ENGINE} run --rm --network "${PERF_NETWORK}" "${KAFKA_TOOL_IMAGE}"  \
      bin/kafka-topics.sh --delete --topic "${TOPIC}" --bootstrap-server "${ENDPOINT}"
}

warmUp() {
  echo -e "${YELLOW}Running warm up${NO_COLOR}"
  producerPerf "$1" "$2" "${WARM_UP_NUM_RECORDS_PRE_TEST}" /dev/null > /dev/null
  consumerPerf "$1" "$2" "${WARM_UP_NUM_RECORDS_PRE_TEST}" /dev/null > /dev/null
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

  echo -e "${YELLOW}Running producer test${NO_COLOR}"

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
  ${CONTAINER_ENGINE} run --rm --network "${PERF_NETWORK}" "${KAFKA_TOOL_IMAGE}" \
      bin/kafka-producer-perf-test.sh --topic "${TOPIC}" --throughput -1 --num-records "${NUM_RECORDS}" --record-size "${RECORD_SIZE}" \
      --producer-props ${PRODUCER_PROPERTIES} bootstrap.servers="${ENDPOINT}" | \
      jq --raw-input --arg name "${TEST_NAME}" --arg commit_id "${COMMIT_ID}" '[.,inputs] | [.[] | match("^(?<sent>\\d+) *records sent" +
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

  echo -e "${YELLOW}Running consumer test${NO_COLOR}"

  # Input:
  # start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
  # 2024-02-21 19:36:23:839, 2024-02-21 19:36:24:256, 0.9766, 2.3419, 1000, 2398.0815, 364, 53, 18.4257, 18867.9245  # Output:
  # Output
  # [
  #  { "sent": 204796, "rate_rps": 40959.2, "rate_mips": 40.00, "avg_lat_ms": 627.9, "max_lat_ms": 759.0 },
  #  { "sent": 300000, "rate_rps": 43184.108248, "rate_mips": 42.17, "avg_lat_ms": 627.62, "max_lat_ms": 759.00,
  #    "percentile50": 644, "percentile95": 744, "percentile99": 753, "percentile999": 758 }
  # ]

  ${CONTAINER_ENGINE} run --rm --network "${PERF_NETWORK}" "${KAFKA_TOOL_IMAGE}"  \
      bin/kafka-consumer-perf-test.sh --topic "${TOPIC}" --messages "${NUM_RECORDS}" --hide-header \
      --bootstrap-server "${ENDPOINT}" |
       jq --raw-input --arg name "${TEST_NAME}" --arg commit_id "${COMMIT_ID}"  '[.,inputs] | [.[] | match("^(?<start.time>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}:\\d{3}), " +
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

  producerPerf "${ENDPOINT}" "${TOPIC}" "${NUM_RECORDS}" "${PRODUCER_RESULT}"

  consumerPerf "${ENDPOINT}" "${TOPIC}" "${NUM_RECORDS}" "${CONSUMER_RESULT}"

  doDeleteTopic "${ENDPOINT}" "${TOPIC}"
}
