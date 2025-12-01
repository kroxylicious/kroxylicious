#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
#. "${SCRIPT_DIR}/../common-perf.sh"

CFG="${SCRIPT_DIR}/config.yaml"
ENDPOINT=kroxylicious:9092

echo "CFG is: ${CFG}"
setupProxyConfig ${CFG}
runDockerCompose up --detach --wait kroxylicious

#setKroxyliciousContainerIdPID

ENDPOINT=${ENDPOINT} doPerfTest

#unsetKroxyliciousContainerIdPID

#runDockerCompose rm -s -f kroxylicious


