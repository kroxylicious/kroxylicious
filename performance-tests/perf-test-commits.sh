#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# perf-test-commits is intended to generate a comparable performance tests results across a series of commits.
# It will checkout each commit in turn and build an image for each commit before running `perf-tests.sh` with each generated image.
# By building multiple images and running them in a single session we ensure identical hardware (and thus performance characteristics)
# to hopefully allow us to isolate the performance impact of each commit.
set -eo pipefail 
PERF_TESTS_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source "${PERF_TESTS_DIR}"/../scripts/common.sh

GREEN='\033[0;32m'
NOCOLOR='\033[0m'

FIND_COMMAND="find"
if [ "$OS" = 'Darwin' ] ; then
  GFIND_COMMAND=
  GFIND_COMMAND=$(resolveCommand gfind)
  if  [ -n "${GFIND_COMMAND:-}" ]; then
    FIND_COMMAND=${GFIND_COMMAND}
    ENABLE_REGEX="-regextype posix-extended"
  else
    # for BSD find
    ENABLE_REGEX="-E"
  fi
else
  # for gnu find
  ENABLE_REGEX="-regextype posix-extended"
fi

GIT_REFS=( "$@" )
SHORT_COMMITS=( )

#Cross platform temp directory creation based on https://unix.stackexchange.com/a/84980
CHECKOUT_DIR=${CHECKOUT_DIR:=$(mktemp -d -t kroxyliciousPerfTestCheckout.XXXXXX 2>/dev/null || mktemp -d -t 'kroxyliciousPerfTestCheckout')}
RESULTS_DIR=${RESULTS_DIR:=$(mktemp -d -t kroxyliciousPerfTestResults.XXXXXX 2>/dev/null || mktemp -d -t 'kroxyliciousPerfTestResults')}
JSON_TEMP_DIR=${JSON_TEMP_DIR:=$(mktemp -d -t kroxyliciousPerfTestJson.XXXXXX 2>/dev/null || mktemp -d -t 'kroxyliciousPerfTestJson')}
CLONE_URL=${CLONE_URL:-"$(git config --get remote.origin.url)"}

PRODUCER_PROPERTIES=${PRODUCER_PROPERTIES:-"acks=all"}
export PRODUCER_PROPERTIES

export PUSH_IMAGE=y #remove this once pulling is optional to save some time.
export TEMP_BUILD=y

cloneRepo() {
  cd "${CHECKOUT_DIR}" || exit 127
  git clone -q "${CLONE_URL}" || exit 128
  cd "kroxylicious" || exit 129
}

checkoutCommit() {
  local COMMIT_ID=$1
  echo -e "Checkout ${GREEN}${COMMIT_ID}${NOCOLOR}"
  git checkout --quiet "${COMMIT_ID}"
}

buildImage() {
  local COMMIT_ID=$1
  echo -e "Building image with tag ${GREEN}g_${COMMIT_ID}${NOCOLOR}"
  "${PERF_TESTS_DIR}/../scripts/build-image.sh" -t "g_${COMMIT_ID}" -s '4h' > /dev/null
}

runPerfTest() {
  local COMMIT_ID=$1
  export KIBANA_OUTPUT_DIR=${RESULTS_DIR}/${COMMIT_ID}
  export PROFILING_OUTPUT_DIRECTORY=${RESULTS_DIR}/${COMMIT_ID}/profile
  mkdir -p "${KIBANA_OUTPUT_DIR}"
  mkdir -p "${PROFILING_OUTPUT_DIRECTORY}"
  export KROXYLICIOUS_IMAGE="${REGISTRY_DESTINATION}:g_${COMMIT_ID}"
  echo -e "Running tests using ${GREEN}${KROXYLICIOUS_IMAGE}${NOCOLOR}"
  "${PERF_TESTS_DIR}/perf-tests.sh"
}

mergeResults() {
  echo -e "Merging results in: ${GREEN}${RESULTS_DIR}${NOCOLOR}"
  # shellcheck disable=SC2086 #because ENABLE_REGEX is a flag plus arg we need word splitting here.
  mapfile -t TEST_NAMES < <(${FIND_COMMAND} "${RESULTS_DIR}" -type d ${ENABLE_REGEX} -regex  ".*\/[0-9]{2}-.*" -exec basename {} \; | sort | uniq)

  for TEST_NAME in "${TEST_NAMES[@]}"; do
   echo -e "generating ${GREEN}${TEST_NAME}${NOCOLOR}"
   JQ_COMMAND=".[0]"
   idx=0
   for REF in "${SHORT_COMMITS[@]}"; do
     if [ ${idx} -ne 0 ]; then
       JQ_COMMAND+=" * .[$((idx++))]"
     else
       ((idx++))
     fi
     jq -cn \
      --arg commit "${REF}" \
      --arg key "${TEST_NAME}" \
      --slurpfile producer_results "${RESULTS_DIR}/${REF}/${TEST_NAME}/producer.json" \
      --slurpfile consumer_results "${RESULTS_DIR}/${REF}/${TEST_NAME}/consumer.json" \
      '{producer: {($key): {($commit): $producer_results}}, consumer: {($key): {($commit): $consumer_results}}}' \
     > "${JSON_TEMP_DIR}/${TEST_NAME}-${REF}.json"
   done

   jq  -c -s "${JQ_COMMAND}" "${JSON_TEMP_DIR}/${TEST_NAME}"-*.json > "${RESULTS_DIR}/${TEST_NAME}-all.json"
  done
}

cloneRepo

for REF in "${GIT_REFS[@]}"; do
    checkoutCommit "${REF}"

    SHORT_COMMIT=$(git rev-parse --short HEAD)
    SHORT_COMMITS+=("${SHORT_COMMIT}")
    buildImage "${SHORT_COMMIT}"

    runPerfTest "${SHORT_COMMIT}"
done

mergeResults
