#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Augments the upstream Strimzi test-clients image with jose4j on the classpath.
# Kafka clients 4.1+ eagerly load jose4j during OAUTHBEARER SASL login (KAFKA-20184),
# but the upstream image does not bundle it. Its run.sh exports CLASSPATH and passes it
# straight to `java -cp`, so adding the jar here and pointing CLASSPATH at it is enough.
ARG TEST_CLIENTS_IMAGE
FROM ${TEST_CLIENTS_IMAGE}
USER root
COPY target/extra-libs/jose4j.jar /opt/extra-libs/jose4j.jar
ENV CLASSPATH=/opt/extra-libs/jose4j.jar
USER 1001
