#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

FROM registry.access.redhat.com/ubi9/ubi-minimal:9.6-1758184547

ARG TARGETOS=linux
ARG TARGETARCH
ARG JAVA_VERSION=17
ARG KROXYLICIOUS_VERSION
ARG CONTAINER_USER=kroxylicious
ARG CONTAINER_USER_UID=185

USER root
WORKDIR /opt/kroxylicious-operator

# Download Tini
ENV TINI_VERSION=v0.19.0
ENV TINI_SHA256_AMD64=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c
ENV TINI_SHA256_ARM64=07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81
ENV TINI_SHA256_PPC64LE=3f658420974768e40810001a038c29d003728c5fe86da211cff5059e48cfdfde
ENV TINI_SHA256_S390X=931b70a182af879ca249ae9de87ef68423121b38d235c78997fafc680ceab32d
ENV TINI_DEST=/usr/bin/tini

RUN set -ex; \
    mkdir -p /opt/tini/bin/; \
    if [[ "${TARGETOS}/${TARGETARCH}" = "linux/ppc64le" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-ppc64le -o ${TINI_DEST}; \
        echo "${TINI_SHA256_PPC64LE} *${TINI_DEST}" | sha256sum -c; \
    elif [[ "${TARGETOS}/${TARGETARCH}" = "linux/arm64" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-arm64 -o ${TINI_DEST}; \
        echo "${TINI_SHA256_ARM64} *${TINI_DEST}" | sha256sum -c; \
    elif [[ "${TARGETOS}/${TARGETARCH}" = "linux/s390x" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-s390x -o ${TINI_DEST}; \
        echo "${TINI_SHA256_S390X} *${TINI_DEST}" | sha256sum -c; \
    else \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o ${TINI_DEST}; \
        echo "${TINI_SHA256_AMD64} *${TINI_DEST}" | sha256sum -c; \
    fi; \
    chmod +x ${TINI_DEST}

RUN microdnf -y update \
    && microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install -y \
                java-${JAVA_VERSION}-openjdk-headless \
                openssl \
                shadow-utils \
    && if [[ -n "${CONTAINER_USER}" && "${CONTAINER_USER}" != "root" ]] ; then groupadd -r -g "${CONTAINER_USER_UID}" "${CONTAINER_USER}" && useradd -m -r -u "${CONTAINER_USER_UID}" -g "${CONTAINER_USER}" "${CONTAINER_USER}"; fi \
    && microdnf remove -y shadow-utils \
    && microdnf clean all

ENV JAVA_HOME=/usr/lib/jvm/jre-${JAVA_VERSION}

COPY target/kroxylicious-operator-${KROXYLICIOUS_VERSION}-app/kroxylicious-operator-${KROXYLICIOUS_VERSION}/ .

USER ${CONTAINER_USER_UID}

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/kroxylicious-operator/bin/operator-start.sh" ]