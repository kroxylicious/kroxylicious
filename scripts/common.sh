#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

OS=$(uname)

if [[ -z $CONTAINER_ENGINE ]]; then
  echo "Setting CONTAINER_ENGINE to default: docker"
  CONTAINER_ENGINE=$(command -v docker)
  export CONTAINER_ENGINE
fi
KUBECTL=$(command -v kubectl)
export KUBECTL
MINIKUBE=$(command -v minikube)
export MINIKUBE
KUSTOMIZE=$(command -v kustomize)
export KUSTOMIZE

if [ "$OS" = 'Darwin' ]; then
  # for MacOS
  SED=$(command -v gsed)
else
  # for Linux and Windows
  SED=$(command -v sed)
fi
export SED
