#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail

echo "To produce to kroxylicious:"
echo "kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.0-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server kroxylicious-service:9292 --topic my-topic"
echo "To consume from kroxylicious:"
echo "kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.0-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server kroxylicious-service:9292 --topic my-topic --from-beginning"
