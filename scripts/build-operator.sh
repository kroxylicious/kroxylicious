#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

. "scripts/common.sh"

GIT_HASH="$(git rev-parse HEAD)"
info "building operator image in minikube for commit ${GIT_HASH}"
IMAGE_TAG="${IMAGE_TAG:-dev-git-${GIT_HASH}}"
KROXYLICIOUS_VERSION=${KROXYLICIOUS_VERSION:-$(mvn org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=project.version -q -DforceStdout)}
TARGETARCH=${TARGETARCH:-$(uname -m)}
IMAGE_REGISTRY=${IMAGE_REGISTRY:-"quay.io"}
IMAGE_REGISTRY_ORG=${IMAGE_REGISTRY_ORG:-${QUAY_ORG}}
IMAGE_NAME=${IMAGE_NAME:operator}
minikube image build . -f Dockerfile.operator -t ${IMAGE_REGISTRY}/${IMAGE_REGISTRY_ORG}/${IMAGE_NAME}:${IMAGE_TAG} --build-opt=build-arg=KROXYLICIOUS_VERSION="${KROXYLICIOUS_VERSION}" --build-opt=build-arg=TARGETARCH="${TARGETARCH}"
