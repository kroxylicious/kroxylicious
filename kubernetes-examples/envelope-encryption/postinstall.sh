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
echo -e  "${YELLOW}Please add/update following link to your '/etc/hosts'.${NOCOLOR}"
echo -e  "${YELLOW}${MINIKUBE_IP} minikube${NOCOLOR}"
echo

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

echo -e "${GREEN}Finally let's lift the bonnet and take a look at the kafka underneath${NOCOLOR}"

echo "kubectl -n kafka run consumer -ti --image=${KAF} --rm=true --restart=Never -- kaf consume trades -b ${BOOTSTRAP}"
