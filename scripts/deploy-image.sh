#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/common.sh"
cd "${SCRIPT_DIR}/.."
KROXYLICIOUS_VERSION=$(./mvnw org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=project.version -q -DforceStdout)

if [[ -z $QUAY_ORG ]]; then
  echo "Please set QUAY_ORG, exiting"
  exit 1
fi

IMAGE="quay.io/${QUAY_ORG}/kroxylicious:${KROXYLICIOUS_VERSION}"
${CONTAINER_ENGINE} build -t "${IMAGE}" --build-arg "KROXYLICIOUS_VERSION=${KROXYLICIOUS_VERSION}" .
if [[ -n $PUSH_IMAGE ]]; then
  echo "Pushing image to quay.io"
  ${CONTAINER_ENGINE} login quay.io
  ${CONTAINER_ENGINE} push "${IMAGE}"
else
  echo "PUSH_IMAGE not set, not pushing to quay.io"
fi
