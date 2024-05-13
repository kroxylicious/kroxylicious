#! /usr/bin/env bash

#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
PERF_TESTS_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
COMMITS=( "$@" )

#Cross platform temp directory creation from https://unix.stackexchange.com/a/84980
RESULTS_DIR=${RESULTS_DIR:=$(mktemp -d kroxyliciousPerfTestResults.XXXXXX 2>/dev/null || mktemp -d -t 'kroxyliciousPerfTestResults')}
echo -e "Writing results to: ${GREEN}${RESULTS_DIR}${NOCOLOR}"

GREEN='\033[0;32m'
NOCOLOR='\033[0m'

export PUSH_IMAGE=y #remove this once pulling is optional to save some time.

checkoutCommit() {
  local COMMIT=$1
  echo -e "Checkout ${GREEN}${COMMIT}${NOCOLOR}"
  git checkout --quiet "${COMMIT}"
}

buildImage() {
  echo "Building image"
  "${PERF_TESTS_DIR}/../scripts/build-image.sh" > /dev/null
}

runPerfTest() {
  local COMMIT=$1
  export KIBANA_OUTPUT_DIR=${RESULTS_DIR}/${COMMIT}
  mkdir -p "${KIBANA_OUTPUT_DIR}"
  "${PERF_TESTS_DIR}/perf-tests.sh"
}


for COMMIT in "${COMMITS[@]}"; do
    checkoutCommit "${COMMIT}"

    buildImage

    runPerfTest "${COMMIT}"
done

echo -e "Merging results"
