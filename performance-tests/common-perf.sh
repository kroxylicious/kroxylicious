#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -Eu

setKroxyContainerIdPID () {
    KROXYLICIOUS_CONTAINER_ID=$(docker container list | grep kroxylicious | awk '{print $1}')
    KROXY_PID=$(docker top ${KROXYLICIOUS_CONTAINER_ID} | grep io.kroxylicious.app.Kroxylicious | awk '{print $2}')
}