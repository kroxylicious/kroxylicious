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
ARG CONTAINER_USER=kroxylicious
ARG CONTAINER_USER_UID=185
ENV ROOTFS=/mnt/rootfs

USER root

# Install shadow-utils on the builder so groupadd/useradd are available
RUN microdnf install -y --nodocs shadow-utils

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
    && if [[ -n "${CONTAINER_USER}" && "${CONTAINER_USER}" != "root" ]] ; then groupadd --root ${ROOTFS} -r -g "${CONTAINER_USER_UID}" "${CONTAINER_USER}" && useradd --root ${ROOTFS} -m -r -u "${CONTAINER_USER_UID}" -g "${CONTAINER_USER}" "${CONTAINER_USER}"; fi \
    && microdnf \
        --installroot ${ROOTFS}\
        --noplugins \
        --config /etc/dnf/dnf.conf \
        --setopt=cachedir=/var/cache/microdnf \
        --setopt=reposdir=/etc/yum.repos.d \
        --setopt=varsdir=/etc/dnf \
        clean all

# Download Tini directly into the rootfs
ARG TINI_DEST=${ROOTFS}/usr/bin/tini

COPY --from=quay.io/kroxylicious/tini:0.19.0 /usr/bin/tini ${TINI_DEST}

RUN chmod +x ${TINI_DEST} \
    && mkdir -p ${ROOTFS}/opt/kroxylicious/classpath-plugins && chown ${CONTAINER_USER_UID}:${CONTAINER_USER_UID} ${ROOTFS}/opt/kroxylicious/classpath-plugins

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

WORKDIR /opt/kroxylicious
COPY target/kroxylicious-app-${KROXYLICIOUS_VERSION}-bin/kroxylicious-app-${KROXYLICIOUS_VERSION}/ .

USER ${CONTAINER_USER_UID}

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/kroxylicious/bin/kroxylicious-start.sh" ]

LABEL url="https://kroxylicious.io/"
LABEL vendor="The Kroxylicious Project"
LABEL maintainer="Kroxylicious Maintainers"
