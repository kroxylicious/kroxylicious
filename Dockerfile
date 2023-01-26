#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# This docker file is only intended for development purposes
ARG BUILD_IMAGE=maven:3.8-openjdk-17
ARG RUNTIME_IMAGE=openjdk:11-jdk-slim
LABEL org.opencontainers.image.source="https://github.com/kroxylicious/kroxylicious.git"

# Maven dependencies stage. Allows for caching pulled deps.
FROM ${BUILD_IMAGE} AS dependencies

WORKDIR /build

COPY etc etc
COPY pom.xml pom.xml
COPY krpc-code-gen/pom.xml krpc-code-gen/pom.xml
COPY kroxylicious/pom.xml kroxylicious/pom.xml
COPY integrationtests/pom.xml integrationtests/pom.xml

RUN mvn -q -ntp -B -pl krpc-code-gen -am dependency:go-offline

RUN mvn -q -ntp -B -pl kroxylicious -am dependency:go-offline

# Kroxylicious jars stage.
FROM ${BUILD_IMAGE} AS build

WORKDIR /build

COPY --from=dependencies /root/.m2 /root/.m2

COPY etc etc
COPY pom.xml pom.xml
COPY krpc-code-gen/ krpc-code-gen
COPY kroxylicious/ kroxylicious
COPY integrationtests/pom.xml integrationtests/pom.xml

RUN mvn -q -ntp -B -pl krpc-code-gen install

RUN mvn -q -ntp -Pdist -Dquick -B -pl krpc-code-gen,kroxylicious package

# Final runtime stage
FROM ${RUNTIME_IMAGE}

WORKDIR /app

COPY --from=build /build/kroxylicious/target/kroxylicious-1.0-SNAPSHOT-jar-with-dependencies.jar kroxylicious-1.0-SNAPSHOT.jar
COPY --from=build /build/kroxylicious/example-proxy-config.yml proxy-config.yml

ENTRYPOINT ["java", "-jar", "kroxylicious-1.0-SNAPSHOT.jar"]
CMD ["-c", "proxy-config.yml"]