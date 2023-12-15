#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail

KAF=quay.io/kroxylicious/kaf
BOOTSTRAP=my-cluster-kafka-bootstrap:9092

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NOCOLOR='\033[0m'

MINIKUBE_IP=$(minikube ip 2>/dev/null)

while true
do
   if grep --quiet "^${MINIKUBE_IP}.*minikube" </etc/hosts; then
      echo
      echo -e  "${GREEN}Host minikube resolves okay${NOCOLOR}"
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

VAULT_UI_URL=$(minikube service vault -n vault --url 2>/dev/null | head -1)
echo "Use the Vault UI ${VAULT_UI_URL} (token 'myroottoken') to create an aes256-gcm96 key called 'all'"
echo "or use this command:"
echo "kubectl exec -n vault vault-0 -- vault write -f transit/keys/all"
echo

echo "Then use commands like these to explore topic-encryption."
echo

echo -e "${GREEN}Publish and consumer messages via the proxy${NOCOLOR}"
echo "kaf -b minikube:30192 topic create trades"
echo "echo 'ibm: 999' | kaf -b minikube:30192 produce trades"
echo "kaf -b minikube:30192 consume trades"
echo

echo -e "${GREEN}You can take a look at the encrypted messages on the cluster like this${NOCOLOR}"

echo "kubectl -n kafka run consumer -ti --image=${KAF} --rm=true --restart=Never -- kaf consume trades -b ${BOOTSTRAP}"
