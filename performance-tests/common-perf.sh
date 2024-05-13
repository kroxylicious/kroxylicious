#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -Eu

setKroxyliciousContainerIdPID () {
    KROXYLICIOUS_CONTAINER_ID=$(${CONTAINER_ENGINE} container list | grep kroxylicious | awk '{print $1}')
    KROXYLICIOUS_PID=$(${CONTAINER_ENGINE} top ${KROXYLICIOUS_CONTAINER_ID} | grep io.kroxylicious.app.Kroxylicious | awk '{print $2}')
}

unsetKroxyliciousContainerIdPID () {
  unset KROXYLICIOUS_CONTAINER_ID
  unset KROXYLICIOUS_PID
}