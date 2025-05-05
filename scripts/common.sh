#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -Eu

OS=$(uname)

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NO_COLOUR='\033[0m' # No Color

export GREEN YELLOW RED BLUE NO_COLOUR

resolveCommand () {
  local targetCommand=${1}
  local resolvedCommand
  resolvedCommand=$(command -v "${targetCommand}")
  if [[ -z ${resolvedCommand} ]]; then
    echo -e "${RED}Unable to resolve path to ${targetCommand}${NO_COLOUR}" >&2
    exit 127
  else
    echo "${resolvedCommand}"
  fi
}

CONTAINER_ENGINE=$(resolveCommand "${CONTAINER_ENGINE:-docker}")
export CONTAINER_ENGINE

KUBECTL=$(resolveCommand kubectl)
export KUBECTL
MINIKUBE=$(resolveCommand minikube)
export MINIKUBE
KUSTOMIZE=$(resolveCommand kustomize)
export KUSTOMIZE
HELM=$(resolveCommand helm)
export HELM
GAWK=$(resolveCommand gawk)
export GAWK

if [ "$OS" = 'Darwin' ]; then
  # for MacOS
  SED=$(resolveCommand gsed)
else
  # for Linux and Windows
  SED=$(resolveCommand sed)
fi
export SED

#
# Extracts a registry server from registry destination such as quay.io/podman/stable
# Works with remote address forms described by https://docs.podman.io/en/stable/markdown/podman-push.1.html
#
extractRegistryServer () {
  echo $1 | ${SED} -e 's#\([^:]*:\)\?\(//\)\?\([^/]*\).*#\3#'
}

function info {
  printf "${GREEN}run-operator.sh: ${1}${NO_COLOUR}\n"
}

function error {
  printf "${RED}run-operator.sh: ${1}${NO_COLOUR}\n"
}

function buildPullSpec {
  IMAGE_REGISTRY=${IMAGE_REGISTRY:-"quay.io"}
  IMAGE_REGISTRY_ORG=${IMAGE_REGISTRY_ORG:-${QUAY_ORG}}
  IMAGE_NAME=${IMAGE_NAME:-${1}}
  echo "${IMAGE_REGISTRY}/${IMAGE_REGISTRY_ORG}/${IMAGE_NAME}:${IMAGE_TAG}"
}