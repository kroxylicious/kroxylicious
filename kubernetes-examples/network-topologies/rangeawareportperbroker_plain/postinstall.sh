#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail

STRIMZI_KAFKA=quay.io/strimzi/kafka:0.38.0-kafka-3.6.0
KCAT=quay.io/kroxylicious/kcat:1.7.1
TOPIC=my-topic
BOOTSTRAP=kroxylicious-service:9292

GREEN='\033[0;32m'
NOCOLOUR='\033[0m'

echo "Here are some commands to try things out. Run the producer and consumer in separate terminals"

echo -e "${GREEN}Apache Kafka Client${NOCOLOUR}"

echo "To produce to kroxylicious:"
echo "kubectl -n kroxylicious run java-kafka-producer -ti --image=${STRIMZI_KAFKA} --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server ${BOOTSTRAP} --topic ${TOPIC}"
echo "To consume from kroxylicious:"
echo "kubectl -n kroxylicious run java-kafka-consumer -ti --image=${STRIMZI_KAFKA} --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server ${BOOTSTRAP} --topic ${TOPIC} --from-beginning"

echo -e "${GREEN}Edenhill Kcat (librdkafka)${NOCOLOUR}"

echo "Note: kcat 1.7 -P will send each line as a separate record, but you need to press ^D to actually get kcat to start sending - see  https://github.com/edenhill/kcat/issues/322"
echo "quay.io/kroxylicious/kcat:1.6.0 will send each line as a record on pressing enter."

echo "To produce to kroxylicious:"
echo "kubectl -n kroxylicious run kcat-kafka-producer -ti --image=${KCAT} --rm=true --restart=Never -- -P -t ${TOPIC} -b ${BOOTSTRAP}"
echo "To consume from kroxylicious:"
echo "kubectl -n kroxylicious run kcat-kafka-consumer -ti --image=${KCAT} --rm=true --restart=Never -- -C -t ${TOPIC} -b ${BOOTSTRAP} -o beginning"
echo "To see the node list exposed by the virtual cluster:"
echo "kubectl -n kroxylicious run node-list -ti --image=quay.io/kroxylicious/kaf --rm=true --restart=Never -- kaf node ls -b ${BOOTSTRAP}"

