#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

#####
# Stage 1: Establish clean ubi9-micro rootfs with its own RPM database
#####
FROM registry.access.redhat.com/ubi9/ubi-micro:9.9-1794478307@sha256:dbd325b96bcd432dd42816d31412bdf1dc0fb973b79ed310344bc23c76c068db AS base

#####
# Stage 2: Install runtime dependencies into the ubi9-micro rootfs using dnf --installroot
#####
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.9-1795198179@sha256:08d4c70f1baf6537a0728656cf7942d561e74a35916509d205b6519861266352 AS builder

ARG JAVA_VERSION=21
ARG CONTAINER_USER=kroxylicious
ARG CONTAINER_USER_UID=185
ARG ROOTFS=/mnt/rootfs

USER root

# Install shadow-utils on the builder so groupadd/useradd are available
RUN microdnf install -y --nodocs shadow-utils

# Copy the ubi9-micro rootfs (including its RPM database) as the install root
COPY --from=base / ${ROOTFS}

# Require the RHSA-2026:100567 fix for CVE-2026-114938 when building with OpenJDK 21.
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
    && if [ "${JAVA_VERSION}" = "21" ]; then \
        minimum_evr='1:21.0.13.1.2-1.2.el9'; \
        installed_evr=$(rpm --root "${ROOTFS}" -q --qf '%{EPOCHNUM}:%{VERSION}-%{RELEASE}' java-21-openjdk-headless) || exit 1; \
        comparison=$(rpm --define "installed_evr ${installed_evr}" --define "minimum_evr ${minimum_evr}" \
            --eval '%{lua:print(rpm.vercmp(rpm.expand("%{installed_evr}"), rpm.expand("%{minimum_evr}")))}') || exit 1; \
        case "${comparison}" in \
            0|1) ;; \
            *) echo "OpenJDK RPM ${installed_evr} does not meet minimum ${minimum_evr} for CVE-2026-114938" >&2; exit 1 ;; \
        esac; \
    fi \
    && if [[ -n "${CONTAINER_USER}" && "${CONTAINER_USER}" != "root" ]] ; then groupadd --root ${ROOTFS} -r -g "${CONTAINER_USER_UID}" "${CONTAINER_USER}" && useradd --root ${ROOTFS} -M -r -u "${CONTAINER_USER_UID}" -g "${CONTAINER_USER}" "${CONTAINER_USER}"; fi \
    && microdnf \
        --installroot ${ROOTFS}\
        --noplugins \
        --config /etc/dnf/dnf.conf \
        --setopt=cachedir=/var/cache/microdnf \
        --setopt=reposdir=/etc/yum.repos.d \
        --setopt=varsdir=/etc/dnf \
        clean all

# Copy Tini into the rootfs from our own image, which is a manifest list, so the
# binary matches whichever platform this image is being built for.
ARG TINI_DEST=${ROOTFS}/usr/bin/tini

COPY --from=quay.io/kroxylicious/tini:0.19.0@sha256:c4558590888cde324d905711c82009c8b5d76cf33f0f39a2435c43cf3dcd2e13 /usr/bin/tini ${TINI_DEST}

RUN chmod +x ${TINI_DEST} \
    && mkdir -p ${ROOTFS}/opt/kroxylicious/classpath-plugins && chown ${CONTAINER_USER_UID}:${CONTAINER_USER_UID} ${ROOTFS}/opt/kroxylicious/classpath-plugins

#####
# Stage 3: Build the final container image on ubi9-micro base
#####
FROM registry.access.redhat.com/ubi9/ubi-micro:9.9-1794478307@sha256:dbd325b96bcd432dd42816d31412bdf1dc0fb973b79ed310344bc23c76c068db

ARG KROXYLICIOUS_VERSION
ARG CONTAINER_USER_UID=185
ARG JAVA_VERSION=21

ARG ROOTFS=/mnt/rootfs
ENV JAVA_HOME=/usr/lib/jvm/jre-${JAVA_VERSION}

COPY --from=builder ${ROOTFS} /

WORKDIR /opt/kroxylicious
COPY target/kroxylicious-app-${KROXYLICIOUS_VERSION}-bin/kroxylicious-app-${KROXYLICIOUS_VERSION}/ .

USER ${CONTAINER_USER_UID}

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/kroxylicious/bin/kroxylicious-start.sh" ]

LABEL url="https://kroxylicious.io/"
LABEL vendor="The Kroxylicious Project"
LABEL maintainer="Kroxylicious Maintainers"
