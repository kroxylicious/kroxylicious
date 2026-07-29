#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <kroxylicious.yml> <release-version>" >&2
    exit 1
fi

CONFIG_PATH=$1
RELEASE_VERSION=$2
SED_COMMAND=${SED:-sed}

if [[ ! -f ${CONFIG_PATH} ]]; then
    echo "Configuration file not found: ${CONFIG_PATH}" >&2
    exit 1
fi

is_semver() {
    [[ $1 =~ ^[0-9]+(\.[0-9]+){2}$ ]]
}

version_is_at_least() {
    local candidate=$1
    local current=$2
    local -a candidate_parts
    local -a current_parts
    local index
    local candidate_part
    local current_part

    IFS=. read -r -a candidate_parts <<< "${candidate}"
    IFS=. read -r -a current_parts <<< "${current}"

    for index in 0 1 2; do
        candidate_part=$((10#${candidate_parts[index]}))
        current_part=$((10#${current_parts[index]}))
        if (( candidate_part > current_part )); then
            return 0
        elif (( candidate_part < current_part )); then
            return 1
        fi
    done

    return 0
}

CURRENT_LATEST_RELEASE=$("${SED_COMMAND}" -n -e 's/^[[:space:]]*latestRelease:[[:space:]]*\([^[:space:]]*\)[[:space:]]*$/\1/p' "${CONFIG_PATH}")

if ! is_semver "${CURRENT_LATEST_RELEASE}"; then
    echo "Invalid latestRelease in ${CONFIG_PATH}: ${CURRENT_LATEST_RELEASE}" >&2
    exit 1
fi

if ! is_semver "${RELEASE_VERSION}"; then
    echo "Invalid release version: ${RELEASE_VERSION}" >&2
    exit 1
fi

if [[ ${RELEASE_VERSION} == "${CURRENT_LATEST_RELEASE}" ]]; then
    echo "Latest release is already ${CURRENT_LATEST_RELEASE}"
elif version_is_at_least "${RELEASE_VERSION}" "${CURRENT_LATEST_RELEASE}"; then
    echo "Updating latest release from ${CURRENT_LATEST_RELEASE} to ${RELEASE_VERSION}"
    "${SED_COMMAND}" -i.bak -e "s/^[[:space:]]*latestRelease:.*$/latestRelease: ${RELEASE_VERSION}/" "${CONFIG_PATH}"
    rm -f "${CONFIG_PATH}.bak"
else
    echo "Keeping latest release at ${CURRENT_LATEST_RELEASE}; ${RELEASE_VERSION} is older"
fi
