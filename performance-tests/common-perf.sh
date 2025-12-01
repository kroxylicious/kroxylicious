#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -Eu

runDockerCompose () {
  #  Docker compose can't see $UID so need to set here before calling it
  ${CONTAINER_ENGINE} compose -f "${PERF_TESTS_DIR}/docker-compose.yaml" "${@}"
}

setupProxyConfig() {
  local kroxylicious_config=${1}
  cp "${kroxylicious_config}" "${PERF_TESTS_DIR}/proxy-config.yaml"
}