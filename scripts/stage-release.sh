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
BRANCH_FROM="main"
WORK_BRANCH_NAME="release-work-$(openssl rand -hex 12)"
RELEASE_NOTES_DIR=${RELEASE_NOTES_DIR:-.releaseNotes}
CHANGELOG_LINK_PREFIX="https://github.com/kroxylicious/kroxylicious"
while getopts ":l:v:b:k:r:n:w:c:h" opt; do
  case $opt in
    v) RELEASE_VERSION="${OPTARG}"
    ;;
    n) NEXT_VERSION="${OPTARG}"
    ;;
    b) BRANCH_FROM="${OPTARG}"
    ;;
    r) REPOSITORY="${OPTARG}"
    ;;
    k) GPG_KEY="${OPTARG}"
    ;;
    l) RELCAND_ID_LABEL="${OPTARG}"
    ;;
    w) WORK_BRANCH_NAME="${OPTARG}"
    ;;
    c) CHANGELOG_LINK_PREFIX="${OPTARG}"
    ;;
    h)
      1>&2 cat << EOF
usage: $0 -k keyid -v version -l relcand-label [-b branch] [-r repository] [-c changelog-link-prefix] [-h]
 -k short key id used to sign the release
 -v version number e.g. 0.3.0
 -b branch to release from (defaults to 'main')
 -n development version e.g. 0.4.0-SNAPSHOT
 -l Release candidate label to be applied to the PR.
 -r the remote name of the kroxylicious repository (defaults to 'origin')
 -w release work branch
 -c URL prefix for issue/PR links in the changelog (defaults to https://github.com/kroxylicious/kroxylicious)
 -h this help message
EOF
      exit 1
    ;;
    \?) echo "Invalid option -$opt ${OPTARG}" >&2
    exit 1
    ;;
  esac
done

if [[ -z "${RELCAND_ID_LABEL}" ]]; then
    echo "No run id label. Please specify -l <run id label>" 1>&2
    exit 1
fi

if [[ -z "${GPG_KEY}" ]]; then
    echo "GPG_KEY not set unable to sign the release. Please specify -k <YOUR_GPG_KEY>" 1>&2
    exit 1
fi

if [[ -z ${RELEASE_VERSION} ]]; then
  echo "No version specified, aborting"
  exit 1
fi

if ! command -v gh &> /dev/null
then
    echo "gh command could not be found."
    exit 1
fi

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

TEMPORARY_RELEASE_BRANCH=""
PREPARE_DEVELOPMENT_BRANCH=""
ORIGINAL_GH_DEFAULT_REPO=""
ORIGINAL_WORKING_BRANCH=$(git branch --show-current)

replaceInFile() {
  local EXPRESSION=$1
  local FILE=$2
  ${SED} -E -i -e "${EXPRESSION}" "${FILE}"
  git add "${FILE}"
}

updateVersionInBenchmarks() {
  replaceInFile "s|KROXYLICIOUS_VERSION:-[0-9]+\.[0-9]+\.[0-9]+|KROXYLICIOUS_VERSION:-${1}|g" \
    kroxylicious-openmessaging-benchmarks/scripts/setup-cluster.sh
}

cleanup() {
    if [[ -n ${ORIGINAL_WORKING_BRANCH} ]]; then
        git checkout "${ORIGINAL_WORKING_BRANCH}" || true
    fi

    if [[ ${ORIGINAL_GH_DEFAULT_REPO} ]]; then
      gh repo set-default ${ORIGINAL_GH_DEFAULT_REPO}
    fi

    # Note that git branch -D echos the sha of the deleted branch to
    # stdout.  This is great for debugging the release process as it
    # lets the developer restore to the state of the tree.
    if [[ ${TEMPORARY_RELEASE_BRANCH} ]]; then
        git branch -D "${TEMPORARY_RELEASE_BRANCH}" || true
    fi

    if [[ ${PREPARE_DEVELOPMENT_BRANCH} ]]; then
        git branch -D "${PREPARE_DEVELOPMENT_BRANCH}" || true
    fi
}

updateVersions() {
  local FROM_VERSION=$1
  local NEW_VERSION=$2
  mvn --quiet --batch-mode versions:set -DnewVersion="${NEW_VERSION}" -DgenerateBackupPoms=false -DprocessAllModules=true

  git add '**/*.yaml' '**/pom.xml' 'pom.xml'
}

trap cleanup EXIT

git stash --all
echo "Creating release branch from ${BRANCH_FROM}"
git fetch -q "${REPOSITORY}"
INITIAL_VERSION=$(mvn help:evaluate -Dexpression=project.version --quiet -DforceStdout)

TEMPORARY_RELEASE_BRANCH="${WORK_BRANCH_NAME}-rel"
git checkout -b "${TEMPORARY_RELEASE_BRANCH}" "${REPOSITORY}/${BRANCH_FROM}"

echo "Versioning Kroxylicious as ${RELEASE_VERSION}"
updateVersions "${INITIAL_VERSION}" "${RELEASE_VERSION}"
${SED} -i "s|\\\${changelog.link.prefix}|${CHANGELOG_LINK_PREFIX}|g" changelog/.templates/CHANGELOG.md
mvn --quiet logchange:release
git checkout -- changelog/.templates/CHANGELOG.md
git add changelog/ CHANGELOG.md

replaceInFile "s_:KroxyliciousVersion:.*_:KroxyliciousVersion: ${RELEASE_VERSION}_g" kroxylicious-docs/docs/_assets/attributes.adoc
replaceInFile "s_:KroxyliciousGitRef:.*_:KroxyliciousGitRef: v${RELEASE_VERSION}_g" kroxylicious-docs/docs/_assets/attributes.adoc

replaceInFile "s_image: 'quay.io/kroxylicious/proxy:.*'_image: 'quay.io/kroxylicious/proxy:${RELEASE_VERSION}'_g" compose/kafka-compose.yaml

updateVersionInBenchmarks "${RELEASE_VERSION}"
replaceInFile "s_quay\.io/kroxylicious/proxy:[^}]*_quay.io/kroxylicious/proxy:${RELEASE_VERSION}_g" performance-tests/docker-compose.yaml

echo "Validating things still build"
mvn --quiet --batch-mode clean install --activate-profiles quick

RELEASE_TAG="v${RELEASE_VERSION}"

echo "Committing release to git"
git commit --message "Release version ${RELEASE_TAG}" --signoff

echo "Deploying release"

MVN_DEPLOY_OUTPUT=$(mktemp)
mvn --activate-profiles release,dist -DskipTests=true -DskipDocs=true -DskipContainerImageBuild=true -DreleaseSigningKey="${GPG_KEY}" -DprocessAllModules=true deploy | tee ${MVN_DEPLOY_OUTPUT}
DEPLOYMENT_ID=$(awk -F'[ .]' '/Uploaded bundle successfully/ {print $9}' < ${MVN_DEPLOY_OUTPUT})

if [[ -z "${DEPLOYMENT_ID}" ]]; then
     echo "Failed to find Central Publishing Portal deployment id in Maven deploy output" 1>&2
     exit 1
fi

echo "Found Central Publishing Portal deployment id: ${DEPLOYMENT_ID}"
echo "${DEPLOYMENT_ID}" > DEPLOYMENT.ID

echo "Release deployed. Extracting release notes in: ${RELEASE_NOTES_DIR}"
mkdir -p "${RELEASE_NOTES_DIR}"
csplit --silent --prefix "${RELEASE_NOTES_DIR}/release-notes_" CHANGELOG.md "/^## /" '{*}'

echo "Preparing for development of ${NEXT_VERSION}"
PREPARE_DEVELOPMENT_BRANCH="${WORK_BRANCH_NAME}"
git checkout -b "${PREPARE_DEVELOPMENT_BRANCH}" "${TEMPORARY_RELEASE_BRANCH}"

updateVersions "${RELEASE_VERSION}" "${NEXT_VERSION}"

# bump the docs for the development version
replaceInFile "s_:KroxyliciousVersion:.*_:KroxyliciousVersion: ${NEXT_VERSION}_g" kroxylicious-docs/docs/_assets/attributes.adoc
replaceInFile "s_:KroxyliciousGitRef:.*_:KroxyliciousGitRef: ${BRANCH_FROM}_g" kroxylicious-docs/docs/_assets/attributes.adoc

replaceInFile "s_image: 'quay.io/kroxylicious/proxy:.*'_image: 'quay.io/kroxylicious/proxy:${NEXT_VERSION}'_g" compose/kafka-compose.yaml

updateVersionInBenchmarks "${NEXT_VERSION}"
replaceInFile "s_quay\.io/kroxylicious/proxy:[^}]*_quay.io/kroxylicious/proxy:${NEXT_VERSION}_g" performance-tests/docker-compose.yaml

# bump the reference version in kroxylicious-api
mvn --quiet --batch-mode --projects :kroxylicious-api versions:set-property -Dproperty="ApiCompatability.ReferenceVersion" -DnewVersion="${RELEASE_VERSION}" -DgenerateBackupPoms=false
# reset kroxylicious-api to enable semver checks if they have been disabled
mvn --quiet --batch-mode --projects :kroxylicious-api versions:set-property -Dproperty="ApiCompatability.EnforceForMajorVersionZero" -DnewVersion="true" -DgenerateBackupPoms=false
git add kroxylicious-api/pom.xml

git commit --message "Start next development version" --signoff


ORIGINAL_GH_DEFAULT_REPO=$(gh repo set-default -v | (grep -v 'no default repository' || true))
gh repo set-default "$(git remote get-url "${REPOSITORY}")"

BODY="Release version ${RELEASE_VERSION}"

# Workaround https://github.com/cli/cli/issues/2691
git push "${REPOSITORY}" HEAD

echo "Creating pull request to merge the released version."
gh pr create --head "${PREPARE_DEVELOPMENT_BRANCH}" \
             --base "${BRANCH_FROM}" \
             --title "Kroxylicious release version ${RELEASE_VERSION} development version ${NEXT_VERSION}" \
             --body "${BODY}" \
             --repo "$(gh repo set-default -v)" \
             --label "${RELCAND_ID_LABEL}"


