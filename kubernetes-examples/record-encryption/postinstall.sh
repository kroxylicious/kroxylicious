#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NOCOLOR='\033[0m'

MINIKUBE_IP=$(minikube ip 2>/dev/null)

while true
do
   if grep --quiet "^${MINIKUBE_IP}.*minikube" </etc/hosts; then
      echo
      echo -e  "${GREEN}Host minikube resolves to ${MINIKUBE_IP}${NOCOLOR}"
      break
   else
     if [[ -z "${FIRST_LOOP}" ]]; then
        echo -e  "${YELLOW}Please make the following the entry in your '/etc/hosts' (use another terminal).${NOCOLOR}"
        echo -e  "${YELLOW}${MINIKUBE_IP} minikube${NOCOLOR}"
        echo -n "Waiting for /etc/hosts update to be made."
        FIRST_LOOP=1
     fi
   fi

   sleep 1
   echo -n .
done

