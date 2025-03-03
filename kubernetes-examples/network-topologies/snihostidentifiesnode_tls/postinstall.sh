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

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/../../../scripts/common.sh"

KAFKA_TOOL_SUFFIX=".sh"
if [ "$OS" = 'Darwin'  ]; then
  if brew --prefix kafka 1>/dev/null 2>/dev/null; then
     KAFKA_TOOL_SUFFIX=""
  fi
fi

DNS_NAMES=$(${KUBECTL} get certificate -n kroxylicious server-certificate -o json | jq -r '.spec.dnsNames | join(" ")')
BOOTSTRAP=$(${KUBECTL} get certificate -n kroxylicious server-certificate -o json | jq -r '.spec.dnsNames[0]')

echo 'Please start "minikube tunnel" in another terminal.'

FIRST_LOOP=0
while true
do
   EXTERNAL_IP=$(${KUBECTL} get service/kroxylicious-service -n kroxylicious -o json | jq -r '.status.loadBalancer.ingress[0].ip // empty')
   if [[ "${EXTERNAL_IP}" ]]; then
      echo
      break
   else
     if [[ -z "${FIRST_LOOP}" ]]; then
        echo -n "Waiting for tunnel to start."
        FIRST_LOOP=1
     fi
   fi

   sleep 1
   echo -n .
done

echo -e  "${GREEN}Found external IP ${EXTERNAL_IP} of load balancer service.${NOCOLOR}"

echo -e  "${YELLOW}Please add following link to your '/etc/hosts'.${NOCOLOR}"
echo -e  "${YELLOW}${EXTERNAL_IP} ${DNS_NAMES}${NOCOLOR}"

common_args=("--bootstrap-server" "${BOOTSTRAP}:9092" "--topic" "my-topic")
producer_args=("${common_args[@]}")
consumer_args=("${common_args[@]}")
consumer_args+=("--from-beginning")

props=("ssl.truststore.type=PEM" "security.protocol=SSL" 'ssl.truststore.location=<(kubectl get secret -n kroxylicious kroxy-server-key-material -o json | jq -r ".data.\"tls.crt\" | @base64d")')

for prop in "${props[@]}"
do
    producer_args+=("--producer-property" "${prop}")
    consumer_args+=("--consumer-property" "${prop}")
done

echo "Now run kafka commands like this:"
cat << EOF
kafka-console-producer${KAFKA_TOOL_SUFFIX} ${producer_args[@]}
kafka-console-consumer${KAFKA_TOOL_SUFFIX} ${consumer_args[@]}
EOF


