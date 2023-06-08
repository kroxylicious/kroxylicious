#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

OS=$(uname)

if [[ -z $CONTAINER_ENGINE ]]; then
  echo "Setting CONTAINER_ENGINE to default: docker"
  CONTAINER_ENGINE=$(which docker)
  export CONTAINER_ENGINE
fi
KUBECTL=$(which kubectl)
export KUBECTL
MINIKUBE=$(which minikube)
export MINIKUBE

if [ "$OS" = 'Darwin' ]; then
  # for MacOS
  SED=$(which gsed)
else
  # for Linux and Windows
  SED=$(which sed)
fi
export SED
