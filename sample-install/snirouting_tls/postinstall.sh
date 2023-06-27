#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/../../scripts/common.sh"

KAFKA_TOOL_SUFFIX=".sh"
if [ "$OS" = 'Darwin'  ]; then
  if brew --prefix kafka 1>/dev/null 2>/dev/null; then
     KAFKA_TOOL_SUFFIX=""
  fi
fi

echo 'Please start "minikube tunnel" in another terminal.'

while true
do
   EXTERNAL_IP=$(${KUBECTL} get service/kroxylicious-service -n kafka -o json | jq -r '.status.loadBalancer.ingress[0].ip // empty')
   if [[ "${EXTERNAL_IP}" ]]; then
      echo
      echo "Found external IP ${EXTERNAL_IP} of load balancer service."
      echo "Please add following link to your '/etc/hosts'."
      echo "${EXTERNAL_IP} mycluster.kafka broker0.mycluster.kafka broker1.mycluster.kafka broker2.mycluster.kafka"
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

common_args=('--bootstrap-server' 'mycluster.kafka:9092' '--topic' 'my-topic')
producer_args=("${common_args[@]}")
consumer_args=("${common_args[@]}")
consumer_args+=('--from-beginning')

props=('ssl.truststore.type=PEM' 'security.protocol=SSL' 'ssl.truststore.location=<(kubectl get secret -n kafka kroxy-server-tls -o json | jq -r ".data.\"tls.crt\" | @base64d")')

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


