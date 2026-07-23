#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
TEST_DIR=$(mktemp -d)
trap 'rm -rf "${TEST_DIR}"' EXIT

run_case() {
    local name=$1
    local current=$2
    local release=$3
    local expected=$4
    local config_path="${TEST_DIR}/${name}.yml"
    local actual

    printf '# The version number of the latest release\nlatestRelease: %s\n' "${current}" > "${config_path}"

    "${SCRIPT_DIR}/update-latest-docs-release.sh" "${config_path}" "${release}"

    actual=$(sed -n -e 's/^latestRelease:[[:space:]]*//p' "${config_path}")
    if [[ ${actual} != "${expected}" ]]; then
        echo "${name}: expected ${expected}, got ${actual}" >&2
        return 1
    fi
}

run_case older-patch 0.22.0 0.20.1 0.22.0
run_case newer-release 0.22.0 0.23.0 0.23.0

echo "update-latest-docs-release tests passed"
