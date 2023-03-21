#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -e

REPOSITORY="origin"
BRANCH_FROM="main"
while getopts ":a:f:" opt; do
  case $opt in
    a) RELEASE_API_VERSION="$OPTARG"
    ;;
    f) RELEASE_VERSION="$OPTARG"
    ;;

    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac

  case $OPTARG in
    -*) echo "Option $opt needs a valid argument"
    exit 1
    ;;
  esac
done

git stash --all
echo "Creating release branch from ${BRANCH_FROM}"
git fetch -q "${REPOSITORY}"
git branch prepare-release "${REPOSITORY}/${BRANCH_FROM}"

if [[ -n ${RELEASE_API_VERSION} ]]; then
  echo "Releasing Public APIs as ${RELEASE_API_VERSION}"
  ./bin/release-api.sh "${RELEASE_API_VERSION}"
fi

if [[ -n ${RELEASE_VERSION} ]]; then
  echo "Releasing Kroxylicious as ${RELEASE_VERSION}"
  ./bin/release-framework.sh "${RELEASE_VERSION}"
fi

if [[ -z ${RELEASE_API_VERSION} && -z ${RELEASE_VERSION} ]]; then
  echo "No versions specified aborting"
  exit 1
fi

if ! command -v gh &> /dev/null
then
    echo "gh command could not be found. Please create a pull request by hand https://github.com/kroxylicious/kroxylicious/compare"
    exit
fi

TITLE=""
BODY=""
if [[ -n ${RELEASE_API_VERSION} ]]; then
  TITLE="${TITLE} Release API v${RELEASE_API_VERSION}"
  BODY="${BODY} Release API version ${RELEASE_API_VERSION}"
fi

if [[ -n ${RELEASE_VERSION} ]]; then
  TITLE="${TITLE} Release v${RELEASE_VERSION}"
  BODY="${BODY} release version ${RELEASE_VERSION}"
fi

gh pr create --base main --title "${TITLE}" --body "${BODY}"
