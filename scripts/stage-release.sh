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
SKIP_VALIDATION="false"
RELEASE_NOTES_DIR=${RELEASE_NOTES_DIR:-.releaseNotes}
while getopts ":l:v:b:k:r:n:w:sh" opt; do
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
    s) SKIP_VALIDATION="true"
    ;;
    h)
      1>&2 cat << EOF
usage: $0 -k keyid -v version -l relcand-label [-b branch] [-r repository] [-s] [-d] [-h]
 -k short key id used to sign the release
 -v version number e.g. 0.3.0
 -b branch to release from (defaults to 'main')
 -n development version e.g. 0.4.0-SNAPSHOT
 -l Release candidate label to be applied to the PR.
 -r the remote name of the kroxylicious repository (defaults to 'origin')
 -w release work branch
 -s skips validation
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
  ${SED} -i -e "${EXPRESSION}" "${FILE}"
  git add "${FILE}"
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
  mvn -q -B versions:set -DnewVersion="${NEW_VERSION}" -DgenerateBackupPoms=false -DprocessAllModules=true

  git add '**/*.yaml' '**/pom.xml' 'pom.xml'
}

trap cleanup EXIT

git stash --all
echo "Creating release branch from ${BRANCH_FROM}"
git fetch -q "${REPOSITORY}"
INITIAL_VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=project.version -q -DforceStdout)

TEMPORARY_RELEASE_BRANCH="${WORK_BRANCH_NAME}-rel"
git checkout -b "${TEMPORARY_RELEASE_BRANCH}" "${REPOSITORY}/${BRANCH_FROM}"

if [[ "${SKIP_VALIDATION:-false}" != true ]]; then
    printf "Validating the build is ${GREEN}green${NC}"
    mvn -q clean verify
fi

echo "Versioning Kroxylicious as ${RELEASE_VERSION}"
updateVersions "${INITIAL_VERSION}" "${RELEASE_VERSION}"
#Set the release version in the Changelog
replaceInFile "s_##\sSNAPSHOT_## ${RELEASE_VERSION//./\\.}_g" CHANGELOG.md

replaceInFile "s_:KroxyliciousVersion:.*_:KroxyliciousVersion: ${RELEASE_VERSION}_g" kroxylicious-docs/docs/_assets/attributes.adoc
replaceInFile "s_:KroxyliciousGitRef:.*_:KroxyliciousGitRef: v${RELEASE_VERSION}_g" kroxylicious-docs/docs/_assets/attributes.adoc

replaceInFile "s_image: 'quay.io/kroxylicious/proxy:.*'_image: 'quay.io/kroxylicious/proxy:${RELEASE_VERSION}'_g" compose/kafka-compose.yaml

echo "Validating things still build"
mvn -q -B clean install -Pquick

RELEASE_TAG="v${RELEASE_VERSION}"

echo "Committing release to git"
git commit --message "Release version ${RELEASE_TAG}" --signoff

git tag -f "${RELEASE_TAG}"

git push "${REPOSITORY}" "${RELEASE_TAG}"

echo "Deploying release"

MVN_DEPLOY_OUTPUT=$(mktemp)
mvn -Prelease,dist -DskipTests=true -DskipDocs=true -DskipContainerImageBuild=true -DreleaseSigningKey="${GPG_KEY}" -DprocessAllModules=true deploy | tee ${MVN_DEPLOY_OUTPUT}
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
# bump the Changelog to the next SNAPSHOT version. We do it this way so the changelog has the new release as the first entry
replaceInFile "s_##\s${RELEASE_VERSION//./\\.}_## SNAPSHOT\n## ${RELEASE_VERSION//./\\.}_g" CHANGELOG.md

# bump the docs for the development version
replaceInFile "s_:KroxyliciousVersion:.*_:KroxyliciousVersion: ${NEXT_VERSION}_g" kroxylicious-docs/docs/_assets/attributes.adoc
replaceInFile "s_:KroxyliciousGitRef:.*_:KroxyliciousGitRef: main_g" kroxylicious-docs/docs/_assets/attributes.adoc # this doesn't make a lot sense...

replaceInFile "s_image: 'quay.io/kroxylicious/proxy:.*'_image: 'quay.io/kroxylicious/proxy:${NEXT_VERSION}'_g" compose/kafka-compose.yaml

# bump the reference version in kroxylicious-api
mvn -q -B -pl :kroxylicious-api versions:set-property -Dproperty="ApiCompatability.ReferenceVersion" -DnewVersion="${RELEASE_VERSION}" -DgenerateBackupPoms=false
# reset kroxylicious-api to enable semver checks if they have been disabled
mvn -q -B -pl :kroxylicious-api versions:set-property -Dproperty="ApiCompatability.EnforceForMajorVersionZero" -DnewVersion="true" -DgenerateBackupPoms=false
git add kroxylicious-api/pom.xml

git commit --message "Start next development version" --signoff


ORIGINAL_GH_DEFAULT_REPO=$(gh repo set-default -v | (grep -v 'no default repository' || true))
gh repo set-default "$(git remote get-url "${REPOSITORY}")"

# create GitHub release via CLI https://cli.github.com/manual/gh_release_create
# it is created as a draft, the deploy_release workflow will publish it.
echo "Creating draft release notes."
API_COMPATABILITY_REPORT=kroxylicious-api/target/japicmp/"${RELEASE_VERSION}"-compatability.html
cp kroxylicious-api/target/japicmp/japicmp.html "${API_COMPATABILITY_REPORT}"
# csplit will create a file for every version as we use ## to denote versions. We also use # CHANGELOG as a header so the current release is actually in the 01 file (zero based)
APP_BINARY_DISTRIBUTION_ASSET="./kroxylicious-app/target/kroxylicious-app-${RELEASE_VERSION}-bin"
OPERATOR_BINARY_DISTRIBUTION_ASSET="./kroxylicious-operator-dist/target/kroxylicious-operator-${RELEASE_VERSION}"
gh release create --title "${RELEASE_TAG}" \
  --notes-file "${RELEASE_NOTES_DIR}/release-notes_01" \
  --draft "${RELEASE_TAG}" \
  "${APP_BINARY_DISTRIBUTION_ASSET}.tar.gz" \
  "${APP_BINARY_DISTRIBUTION_ASSET}.tar.gz.asc" \
  "${APP_BINARY_DISTRIBUTION_ASSET}.zip" \
  "${APP_BINARY_DISTRIBUTION_ASSET}.zip.asc" \
  "${OPERATOR_BINARY_DISTRIBUTION_ASSET}.tar.gz" \
  "${OPERATOR_BINARY_DISTRIBUTION_ASSET}.tar.gz.asc" \
  "${OPERATOR_BINARY_DISTRIBUTION_ASSET}.zip" \
  "${OPERATOR_BINARY_DISTRIBUTION_ASSET}.zip.asc" \
  "${API_COMPATABILITY_REPORT}"


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

