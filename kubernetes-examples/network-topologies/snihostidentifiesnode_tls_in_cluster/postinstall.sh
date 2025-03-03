#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail

echo "to produce run:"
echo 'kubectl exec -it -nkafka my-cluster-dual-role-0 -- bash -c "echo $(kubectl get secret -n kroxylicious kroxy-server-key-material -o json | jq -r ".data.\"tls.crt\"") | base64 -d > /tmp/certo && ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-proxy.kroxylicious:9092 --topic my-topic --producer-property ssl.truststore.type=PEM --producer-property security.protocol=SSL --producer-property ssl.truststore.location=/tmp/certo"'
echo "to consume run:"
echo 'kubectl exec -it -nkafka my-cluster-dual-role-0 -- bash -c "echo $(kubectl get secret -n kroxylicious kroxy-server-key-material -o json | jq -r ".data.\"tls.crt\"") | base64 -d > /tmp/certo && ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-proxy.kroxylicious:9092 --topic my-topic --from-beginning --consumer-property ssl.truststore.type=PEM --consumer-property security.protocol=SSL --consumer-property ssl.truststore.location=/tmp/certo"'
