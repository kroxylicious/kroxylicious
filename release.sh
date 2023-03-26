#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -e

REPOSITORY="origin"
BRANCH_FROM="main"
while getopts ":a:f:b:r:k:" opt; do
  case $opt in
    a) RELEASE_API_VERSION="${OPTARG}"
    ;;
    f) RELEASE_VERSION="${OPTARG}"
    ;;
    b) BRANCH_FROM="${OPTARG}"
    ;;
    r) REPOSITORY="${OPTARG}"
    ;;
    k) GPG_KEY="${OPTARG}"
    ;;

    \?) echo "Invalid option -${OPTARG}" >&2
    exit 1
    ;;
  esac

  case ${OPTARG} in
    -*) echo "Option $opt needs a valid argument"
    exit 1
    ;;
  esac
done

if [[ -z "${GPG_KEY}" ]]; then
    echo "GPG_KEY not set unable to sign the release. Please specify -k <YOUR_GPG_KEY>" 1>&2
    exit 1
fi

if [[ -z ${RELEASE_API_VERSION} && -z ${RELEASE_VERSION} ]]; then
  echo "No versions specified aborting"
  exit 1
fi

GREEN='\033[0;32m'
NC='\033[0m' # No Color

git stash --all
echo "Creating release branch from ${BRANCH_FROM}"
git fetch -q "${REPOSITORY}"
RELEASE_DATE=$(date -u '+%Y-%m-%d')
git checkout -b "prepare-release-${RELEASE_DATE}" "${REPOSITORY}/${BRANCH_FROM}"

#Disable the shell check as the colour codes only work with interpolation.
# shellcheck disable=SC2059
printf "Validating the build is ${GREEN}green${NC}"
mvn -q clean verify

if [[ -n ${RELEASE_API_VERSION} ]]; then
  echo "Versioning Public APIs as ${RELEASE_API_VERSION}"
  ./bin/release-api.sh "${RELEASE_API_VERSION}"
  echo "Versioned the public API"
fi

if [[ -n ${RELEASE_VERSION} ]]; then
  echo "Versioning Kroxylicious as ${RELEASE_VERSION}"
  ./bin/release-framework.sh "${RELEASE_VERSION}"
  echo "Versioned the Framework"
fi

git push --tags

echo "Deploying release to maven central"
mvn deploy -Prelease -DskipTests=true -DreleaseSigningKey="${GPG_KEY}"

if ! command -v gh &> /dev/null
then
    echo "gh command could not be found. Please create a pull request by hand https://github.com/kroxylicious/kroxylicious/compare"
    exit
fi

BODY=""
if [[ -n ${RELEASE_API_VERSION} ]]; then
  BODY="${BODY} Release API version ${RELEASE_API_VERSION}"
fi

if [[ -n ${RELEASE_VERSION} ]]; then
  BODY="${BODY} Release version ${RELEASE_VERSION}"
fi

echo "Create pull request to merge the released version."
gh pr create --base main --title "Kroxylicious Release %{RELEASE_DATE}" --body "${BODY}"
