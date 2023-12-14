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

echo -e "${GREEN}Configure client ${NOCOLOR}"
echo "kaf config add-cluster topencenv -b minikube:30192"
echo

echo -e "${GREEN}Do some work in topencenv${NOCOLOR}"
echo "kaf config use-cluster topencenv"
echo "kaf topic create billingapp"
echo "echo 'hello, world' | kaf produce billingapp"
echo "kaf consume billingapp --group billinggroup"
echo

echo -e "${GREEN}Finally let's lift the bonnet and take a look at the kafka underneath${NOCOLOR}"

echo "kubectl -n kafka run consumer -ti --image=${KEF} --rm=true --restart=Never -- kaf consume billingapp --offset oldest -b ${BOOTSTRAP}"
