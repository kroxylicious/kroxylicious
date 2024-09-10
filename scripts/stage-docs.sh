#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/common.sh"
# Tmp fix - revert the nounset change made by common to match the expectations of this script.
set +u

REPOSITORY="origin"
BRANCH_FROM="main"
DRY_RUN="false"
ORIGINAL_GH_DEFAULT_REPO=""
while getopts ":v:u:b:dh" opt; do
  case $opt in
    v) RELEASE_VERSION="${OPTARG}"
    ;;
    u) WEBSITE_REPO_URL="${OPTARG}"
    ;;
    b) BRANCH_FROM="${OPTARG}"
    ;;
    d) DRY_RUN="true"
    ;;
    h)
      1>&2 cat << EOF
usage: $0 -v version -u url [-b branch] [-r repository] [-d] [-h]
 -v version number e.g. 0.3.0
 -u url of the website repository e.g. git@github.com:kroxylicious/kroxylicious.github.io.git
 -b branch to release from (defaults to 'main')
 -d dry-run mode
 -h this help message
EOF
      exit 1
    ;;
    \?) echo "Invalid option -$opt ${OPTARG}" >&2
    exit 1
    ;;
  esac

done

if [[ -z ${RELEASE_VERSION} ]]; then
  echo "No version specified, aborting"
  exit 1
fi

if [[ -z ${WEBSITE_REPO_URL} ]]; then
  echo "No website repository URL specified, aborting"
  exit 1
fi

BLUE='\033[0;34m'
NC='\033[0m' # No Color

ORIGINAL_WORKING_BRANCH=$(git branch --show-current)

cleanup() {
    if [[ -n ${ORIGINAL_WEBSITE_WORKING_BRANCH} ]]; then
        git checkout "${ORIGINAL_WEBSITE_WORKING_BRANCH}" || true
    fi

    if [[ ${ORIGINAL_GH_DEFAULT_REPO} ]]; then
      gh repo set-default "${ORIGINAL_GH_DEFAULT_REPO}"
    fi

    # Note that git branch -D echos the sha of the deleted branch to
    # stdout.  This is great for debugging the release process as it
    # lets the developer restore to the state of the tree.
    if [[ "${DRY_RUN:-false}" == true && ${RELEASE_DOCS_BRANCH} ]]; then
        git branch -D "${RELEASE_DOCS_BRANCH}" || true
    fi

    cd ../kroxylicious/

    if [[ -n ${ORIGINAL_WORKING_BRANCH} ]]; then
        git checkout "${ORIGINAL_WORKING_BRANCH}" || true
    fi
}

trap cleanup EXIT

git stash --all

RELEASE_DATE=$(date -u '+%Y-%m-%d')
RELEASE_TAG="v${RELEASE_VERSION}"
RELEASE_DOCS_BRANCH="prepare-${RELEASE_TAG}-release-docs-${RELEASE_DATE}"

# Use a `/.` at the end of the source path to avoid the source path being appended to the destination path if the `.../_files/` folder already exists
KROXYLICIOUS_DOCS_LOCATION="../kroxylicious/docs/."
WEBSITE_DOCS_LOCATION="./docs/${RELEASE_TAG}"

if [[ "${DRY_RUN:-false}" == true ]]; then
    #Disable the shell check as the colour codes only work with interpolation.
    # shellcheck disable=SC2059
    printf "${BLUE}Dry-run mode:${NC} no remote tags or PRs will be created, artefacts will be deployed to: ${DEPLOY_DRY_RUN_DIR}\n"
    GIT_DRYRUN="--dry-run"
fi

echo "Checking out tags/${RELEASE_TAG} in  in $(git remote get-url "${REPOSITORY}")"
git checkout "tags/${RELEASE_TAG}"

# Move up a directory so we don't end up with website files in the main repository
cd ../
echo "In '$(pwd)', cloning website repository at ${WEBSITE_REPO_URL}"
git clone "${WEBSITE_REPO_URL}"
cd ./kroxylicious.github.io/

ORIGINAL_WEBSITE_WORKING_BRANCH=$(git branch --show-current)

echo "Creating branch ${RELEASE_DOCS_BRANCH} from ${BRANCH_FROM} in $(git remote get-url "${REPOSITORY}")"
git checkout -b "${RELEASE_DOCS_BRANCH}"

echo "Copying release docs from ${KROXYLICIOUS_DOCS_LOCATION} to ${WEBSITE_DOCS_LOCATION}/_files"
mkdir -p "${WEBSITE_DOCS_LOCATION}/"
cp -R "${KROXYLICIOUS_DOCS_LOCATION}" "${WEBSITE_DOCS_LOCATION}/_files"
# Remove README.md from copied files
rm "${WEBSITE_DOCS_LOCATION}/_files/README.md"

echo "Creating AsciiDoc entrypoint file at ${WEBSITE_DOCS_LOCATION}/index.adoc"
RELEASE_DOCS_INDEX_TEMPLATE="
---
title: Kroxylicious Proxy ${RELEASE_TAG}
---

include::_files/index.adoc[leveloffset=0]
"
echo "${RELEASE_DOCS_INDEX_TEMPLATE}" > "${WEBSITE_DOCS_LOCATION}/index.adoc"

echo "Update _data/kroxylicious.yml to add new version to website navigation"
match="url: '\/kroxylicious'"
insert="  - title: '${RELEASE_TAG}'\n    url: '\/docs\/${RELEASE_TAG}\/'"
KROXYLICIOUS_NAV_FILE="_data/kroxylicious.yml"
${SED} -i "s/$match/$match\n$insert/" "${KROXYLICIOUS_NAV_FILE}"

echo "Committing release documentation to git"
# Commit and push changes to branch in `kroxylicious/kroxylicious.github.io`
git add "${WEBSITE_DOCS_LOCATION}" "${KROXYLICIOUS_NAV_FILE}"
git commit --message "Prepare ${RELEASE_TAG} release documentation" --signoff
git push "${REPOSITORY}" "${RELEASE_DOCS_BRANCH}" ${GIT_DRYRUN:-}

if [[ "${DRY_RUN:-false}" == true ]]; then
    exit 0
fi

if ! command -v gh &> /dev/null
then
    echo "gh command could not be found. Please create a pull request by hand https://github.com/kroxylicious/kroxylicious/compare"
    exit
fi

# Change GitHub CLI to point at website repository
ORIGINAL_GH_DEFAULT_REPO=$(gh repo set-default -v | (grep -v 'no default repository' || true))
gh repo set-default "$(git remote get-url "${REPOSITORY}")"

echo "Creating pull request to publish release documentation to website."
# Open PR to merge branch to `main` in `kroxylicious/kroxylicious.github.io`
BODY="Prepare ${RELEASE_TAG} release documentation for publishing to website"
gh pr create --head "${RELEASE_DOCS_BRANCH}" --base "${BRANCH_FROM}" --title "Kroxylicious ${RELEASE_TAG} release documentation ${RELEASE_DATE}" --body "${BODY}" --repo "$(gh repo set-default -v)"
