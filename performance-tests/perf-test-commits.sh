#! /usr/bin/env bash

#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
PERF_TESTS_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
COMMITS=( "$@" )

#Cross platform temp directory creation based on https://unix.stackexchange.com/a/84980
RESULTS_DIR=${RESULTS_DIR:=$(mktemp -d -t kroxyliciousPerfTestResults.XXXXXX 2>/dev/null || mktemp -d -t 'kroxyliciousPerfTestResults')}
echo -e "Writing results to: ${GREEN}${RESULTS_DIR}${NOCOLOR}"

GREEN='\033[0;32m'
NOCOLOR='\033[0m'

export PUSH_IMAGE=y #remove this once pulling is optional to save some time.
export TEMP_BUILD=y

checkoutCommit() {
  local COMMIT_ID=$1
  echo -e "Checkout ${GREEN}${COMMIT_ID}${NOCOLOR}"
  git checkout --quiet "${COMMIT_ID}"
}

buildImage() {
  local COMMIT_ID=$1
  export KROXYLICIOUS_VERSION="${COMMIT_ID}"
  echo -e "Building image with version ${GREEN}${KROXYLICIOUS_VERSION}${NOCOLOR}"
  "${PERF_TESTS_DIR}/../scripts/build-image.sh" > /dev/null
}

runPerfTest() {
  local COMMIT_ID=$1
  export KIBANA_OUTPUT_DIR=${RESULTS_DIR}/${COMMIT_ID}
  mkdir -p "${KIBANA_OUTPUT_DIR}"
  echo -e "Running tests ${GREEN}${COMMIT_ID}${NOCOLOR}"
  "${PERF_TESTS_DIR}/perf-tests.sh"
}

for COMMIT in "${COMMITS[@]}"; do
    checkoutCommit "${COMMIT}"

    SHORT_COMMIT=$(git rev-parse --short HEAD)

    buildImage "${SHORT_COMMIT}"

    runPerfTest "${SHORT_COMMIT}"
done