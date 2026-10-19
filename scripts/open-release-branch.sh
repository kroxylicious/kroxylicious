#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -e
set -o pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/common.sh"
# Tmp fix - revert the nounset change made by common to match the expectations of this script.
set +u

REPOSITORY="origin"

while getopts ":t:n:b:r:h" opt; do
  case $opt in
    t) RELEASE_TAG="${OPTARG}"
    ;;
    n) DEV_VERSION="${OPTARG}"
    ;;
    b) BRANCH_NAME="${OPTARG}"
    ;;
    r) REPOSITORY="${OPTARG}"
    ;;
    h)
      1>&2 cat << EOF
usage: $0 -t release-tag -n dev-version -b branch-name [-r repository] [-h]
 -t release tag to branch from, e.g. v0.25.0
 -n next development version, e.g. 0.25.1-SNAPSHOT
 -b release branch name to create, e.g. release/v0.25
 -r remote name (defaults to 'origin')
 -h this help message
EOF
      exit 1
    ;;
    \?) echo "Invalid option -$opt ${OPTARG}" >&2
    exit 1
    ;;
  esac
done

if [[ -z "${RELEASE_TAG}" ]]; then
    echo "No release tag specified. Please specify -t <release-tag>" 1>&2
    exit 1
fi

if [[ -z "${DEV_VERSION}" ]]; then
    echo "No development version specified. Please specify -n <dev-version>" 1>&2
    exit 1
fi

if [[ -z "${BRANCH_NAME}" ]]; then
    echo "No branch name specified. Please specify -b <branch-name>" 1>&2
    exit 1
fi

if [[ ! "${RELEASE_TAG}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Release tag '${RELEASE_TAG}' is not in the expected form (e.g. v0.25.0)" 1>&2
    exit 1
fi

if [[ ! "${DEV_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+-SNAPSHOT$ ]]; then
    echo "Development version '${DEV_VERSION}' is not in the expected form (e.g. 0.25.1-SNAPSHOT)" 1>&2
    exit 1
fi

RELEASE_VERSION="${RELEASE_TAG#v}"

# Checking out the release tag switches the working tree to the tag state,
# which predates update-version-references.sh. Copy it (and common.sh it
# depends on) to a temp dir so they survive the working-tree switch.
_SCRIPT_TMPDIR=$(mktemp -d)
trap 'rm -rf "${_SCRIPT_TMPDIR}"' EXIT
cp "${SCRIPT_DIR}/common.sh" "${_SCRIPT_TMPDIR}/"
cp "${SCRIPT_DIR}/update-version-references.sh" "${_SCRIPT_TMPDIR}/"

echo "Creating release branch ${BRANCH_NAME} from tag ${RELEASE_TAG}"
git fetch -q "${REPOSITORY}" "refs/tags/${RELEASE_TAG}:refs/tags/${RELEASE_TAG}"
git checkout -b "${BRANCH_NAME}" "${RELEASE_TAG}"

echo "Setting development version to ${DEV_VERSION}"
"${_SCRIPT_TMPDIR}/update-version-references.sh" -v "${DEV_VERSION}" -g "${BRANCH_NAME}" -a "${RELEASE_VERSION}"

git commit --message "ci(release): open ${BRANCH_NAME} for development at ${DEV_VERSION}" --signoff

echo "Pushing ${BRANCH_NAME} to ${REPOSITORY}"
git push "${REPOSITORY}" "${BRANCH_NAME}"
