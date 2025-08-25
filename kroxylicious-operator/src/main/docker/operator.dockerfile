#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

FROM registry.access.redhat.com/ubi9/ubi-minimal:9.6-1755695350

ARG JAVA_VERSION=17
ARG KROXYLICIOUS_VERSION
ARG CONTAINER_USER=kroxylicious
ARG CONTAINER_USER_UID=185

USER root
WORKDIR /opt/kroxylicious-operator

RUN microdnf -y update \
    && microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install -y \
                java-${JAVA_VERSION}-openjdk-headless \
                openssl \
                shadow-utils \
    && if [[ -n "${CONTAINER_USER}" && "${CONTAINER_USER}" != "root" ]] ; then groupadd -r -g "${CONTAINER_USER_UID}" "${CONTAINER_USER}" && useradd -m -r -u "${CONTAINER_USER_UID}" -g "${CONTAINER_USER}" "${CONTAINER_USER}"; fi \
    && microdnf remove -y shadow-utils \
    && microdnf clean all

ENV JAVA_HOME=/usr/lib/jvm/jre-${JAVA_VERSION}

COPY --from=quay.io/k_wall/tini:0.19.0 /usr/bin/tini /usr/bin/tini

COPY target/kroxylicious-operator-${KROXYLICIOUS_VERSION}-app/kroxylicious-operator-${KROXYLICIOUS_VERSION}/ .

USER ${CONTAINER_USER_UID}

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/kroxylicious-operator/bin/operator-start.sh" ]