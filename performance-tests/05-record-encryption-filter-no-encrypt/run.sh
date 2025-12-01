#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/../common-perf.sh"

CFG=04-record-encryption-filter/config.yaml
ENDPOINT=kroxylicious:9092

setupProxyConfig "${CFG}"
runDockerCompose up --detach --wait kroxylicious vault

docker exec vault vault secrets enable transit 1>/dev/null
# Don't create a key

ENDPOINT=${ENDPOINT} doPerfTest

runDockerCompose rm -s -f kroxylicious vault




