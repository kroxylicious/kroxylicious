#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail

STRIMZI_KAFKA=quay.io/strimzi/kafka:0.38.0-kafka-3.6.0
BOOTSTRAP=my-cluster-kafka-bootstrap:9092

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NOCOLOR='\033[0m'

MINIKUBE_IP=$(minikube ip)
echo -e  "${YELLOW}Please add/update following link to your '/etc/hosts'.${NOCOLOR}"
echo -e  "${YELLOW}${MINIKUBE_IP} minikube${NOCOLOR}"
echo

echo "Then use commands like these to explore multi-tenancy."
echo

echo -e "${GREEN}Configure two tenants: ${NOCOLOR}"
echo "kaf config add-cluster devenv1 -b minikube:30192"
echo "kaf config add-cluster devenv2 -b minikube:30292"
echo

echo -e "${GREEN}Do some work in devenv1${NOCOLOR}"
echo "kaf config use-cluster devenv1"
echo "kaf topic create billingapp"
echo "kaf topic create foo"
echo "echo 'hello, world' | kaf produce billingapp"
echo "kaf consume billingapp --group billinggroup --commit"
echo "kaf topics"
echo "kaf groups"
echo

echo -e "${GREEN}Now do some work in devenv2${NOCOLOR}"
echo "kaf config use-cluster devenv2"
echo "kaf topics"
echo "etc.."

echo -e "${GREEN}Finally let's lift the bonnet and take a look at the kafka underneath${NOCOLOR}"
echo "kubectl -n kafka run listtopics -ti --image=${STRIMZI_KAFKA} --rm=true --restart=Never -- bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --list"
echo "kubectl -n kafka run listgroups -ti --image=${STRIMZI_KAFKA} --rm=true --restart=Never -- bin/kafka-consumer-groups.sh --bootstrap-server ${BOOTSTRAP} --list"
