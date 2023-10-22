#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -e
REPOSITORY="origin"
BRANCH_FROM="main"
DRY_RUN="false"
SKIP_VALIDATION="false"
while getopts ":v:b:k:r:dsh" opt; do
  case $opt in
    v) RELEASE_VERSION="${OPTARG}"
    ;;
    b) BRANCH_FROM="${OPTARG}"
    ;;
    r) REPOSITORY="${OPTARG}"
    ;;
    k) GPG_KEY="${OPTARG}"
      if [[ -z "${GPG_KEY}" ]]; then
          echo "GPG_KEY not set unable to sign the release. Please specify -k <YOUR_GPG_KEY>" 1>&2
          exit 1
      fi
    ;;
    d) DRY_RUN="true"
    ;;
    s) SKIP_VALIDATION="true"
    ;;
    h)
      1>&2 cat << EOF
usage: $0 -k keyid -v version [-b branch] [-r repository] [-s] [-d] [-h]
 -k short key id used to sign the release
 -v version number e.g. 0.3.0
 -b branch to release from (defaults to 'main')
 -r the remote name of the kroxylicious repository (defaults to `origin`)
 -s skips validation
 -d dry-run mode
 -h this help message
EOF
      exit 1
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

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

ORIGINAL_WORKING_BRANCH=$(git branch --show-current)

cleanup() {
    if [[ -n ${ORIGINAL_WORKING_BRANCH} ]]; then
        git checkout "${ORIGINAL_WORKING_BRANCH}" || true
    fi

    if [[ "${DRY_RUN:-false}" == true && ${TEMPORARY_RELEASE_BRANCH} ]]; then
        # Note that git branch -D echos the sha of the deleted branch to
        # stdout.  This is great for debugging the release process as it
        # lets the developer restore to the state of the tree.
        git branch -D "${TEMPORARY_RELEASE_BRANCH}" || true
    fi

    if [[ "${DRY_RUN:-false}" == true && ${RELEASE_TAG} ]]; then
      git tag --delete "${RELEASE_TAG}" || true
    fi
}

trap cleanup EXIT

git stash --all
echo "Creating release branch from ${BRANCH_FROM}"
git fetch -q "${REPOSITORY}"
RELEASE_DATE=$(date -u '+%Y-%m-%d')
TEMPORARY_RELEASE_BRANCH="prepare-release-${RELEASE_DATE}"
git checkout -b "prepare-release-${RELEASE_DATE}" "${REPOSITORY}/${BRANCH_FROM}"

if [[ "${DRY_RUN:-false}" == true ]]; then
    DEPLOY_DRY_RUN_DIR=$(mktemp -d)
    #Disable the shell check as the colour codes only work with interpolation.
    # shellcheck disable=SC2059
    printf "${BLUE}Dry-run mode:${NC} no remote tags or PRs will be created, artefacts will be deployed to: ${DEPLOY_DRY_RUN_DIR}\n"
    GIT_DRYRUN="--dry-run"
    MVN_DEPLOY_DRYRUN="-DaltDeploymentRepository=ossrh::file:${DEPLOY_DRY_RUN_DIR}"
fi

if [[ "${SKIP_VALIDATION:-false}" != true ]]; then
    printf "Validating the build is ${GREEN}green${NC}"
    mvn -q clean verify
fi

mvn -q versions:set -DnewVersion="${RELEASE_VERSION}" -DgenerateBackupPoms=false -DprocessAllModules=true

echo "Validating things still build"
mvn -q clean install -Pquick


RELEASE_TAG="v${RELEASE_VERSION}"

echo "Committing release to git"
git add '**/pom.xml' 'pom.xml'
git commit --message "Release version v${RELEASE_TAG}" --signoff

git tag -f "${RELEASE_TAG}"

git push "${REPOSITORY}" "${RELEASE_TAG}" ${GIT_DRYRUN:-}

echo "Deploying release to maven central"
mvn deploy -Prelease -DskipTests=true -DreleaseSigningKey="${GPG_KEY}" ${MVN_DEPLOY_DRYRUN:-}

if [[ "${DRY_RUN:-false}" == true ]]; then
    exit 0
fi

if ! command -v gh &> /dev/null
then
    echo "gh command could not be found. Please create a pull request by hand https://github.com/kroxylicious/kroxylicious/compare"
    exit
fi

BODY="Release version ${RELEASE_VERSION}"

echo "Create pull request to merge the released version."
gh pr create --base main --title "Kroxylicious Release ${RELEASE_DATE}" --body "${BODY}"
