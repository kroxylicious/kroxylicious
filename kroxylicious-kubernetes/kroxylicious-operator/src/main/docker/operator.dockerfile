#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

#####
# Stage 1: Establish clean ubi9-micro rootfs with its own RPM database
#####
FROM registry.access.redhat.com/ubi9/ubi-micro:9.8-1791895366@sha256:5be877a2a280ae36eb9ab80ca4efb28bbf77c57abbba38137cdc37a03b8465e6 AS base

#####
# Stage 2: Install runtime dependencies into the ubi9-micro rootfs using dnf --installroot
#####
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.8-1788166357@sha256:7fbeae18dc9476399f565e68255f602a3374ea8614ba3d14843565131a13ff93 AS builder

ARG JAVA_VERSION=21
ARG TARGETOS=linux
ARG TARGETARCH
ARG KROXYLICIOUS_VERSION
ARG CONTAINER_USER=kroxylicious
ARG CONTAINER_USER_UID=185
ENV ROOTFS=/mnt/rootfs

USER root

# Copy the ubi9-micro rootfs (including its RPM database) as the install root
COPY --from=base / ${ROOTFS}

RUN microdnf install \
        --installroot ${ROOTFS} \
        --noplugins \
        --config /etc/dnf/dnf.conf \
        --setopt=cachedir=/var/cache/microdnf \
        --setopt=reposdir=/etc/yum.repos.d \
        --setopt=varsdir=/etc/dnf \
        --setopt=install_weak_deps=0 \
        --setopt=tsflags=nodocs \
        --releasever 9 \
        -y \
        java-${JAVA_VERSION}-openjdk-headless \
        openssl \
        shadow-utils \
    && if [[ -n "${CONTAINER_USER}" && "${CONTAINER_USER}" != "root" ]] ; then groupadd -r -g "${CONTAINER_USER_UID}" "${CONTAINER_USER}" && useradd -m -r -u "${CONTAINER_USER_UID}" -g "${CONTAINER_USER}" "${CONTAINER_USER}"; fi \
    && microdnf remove  \
        --installroot ${ROOTFS} \
        --noplugins \
        --config /etc/dnf/dnf.conf \
        --setopt=cachedir=/var/cache/microdnf \
        --setopt=reposdir=/etc/yum.repos.d \
        --setopt=varsdir=/etc/dnf \
        -y shadow-utils \
    && microdnf \
        --installroot ${ROOTFS} \
        --noplugins \
        --config /etc/dnf/dnf.conf \
        --setopt=cachedir=/var/cache/microdnf \
        --setopt=reposdir=/etc/yum.repos.d \
        --setopt=varsdir=/etc/dnf \
        clean all

# Download Tini directly into the rootfs
ARG TINI_VERSION=v0.19.0
ARG TINI_SHA256_AMD64=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c
ARG TINI_SHA256_ARM64=07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81
ARG TINI_SHA256_PPC64LE=3f658420974768e40810001a038c29d003728c5fe86da211cff5059e48cfdfde
ARG TINI_SHA256_S390X=931b70a182af879ca249ae9de87ef68423121b38d235c78997fafc680ceab32d
ARG TINI_DEST=${ROOTFS}/usr/bin/tini

RUN set -ex; \
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

#####
# Stage 3: Build the final container image on ubi9-micro base
#####
FROM registry.access.redhat.com/ubi9/ubi-micro:9.8-1791895366@sha256:5be877a2a280ae36eb9ab80ca4efb28bbf77c57abbba38137cdc37a03b8465e6

ARG KROXYLICIOUS_VERSION
ARG CONTAINER_USER_UID=185
ARG JAVA_VERSION=21

ENV ROOTFS=/mnt/rootfs
ENV JAVA_HOME=/usr/lib/jvm/jre-${JAVA_VERSION}

COPY --from=builder ${ROOTFS} /

WORKDIR /opt/kroxylicious-operator
COPY target/kroxylicious-operator-${KROXYLICIOUS_VERSION}-app/kroxylicious-operator-${KROXYLICIOUS_VERSION}/ .

USER ${CONTAINER_USER_UID}

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/kroxylicious-operator/bin/operator-start.sh" ]

LABEL url="https://kroxylicious.io/"
LABEL vendor="The Kroxylicious Project"
LABEL maintainer="Kroxylicious Maintainers"
