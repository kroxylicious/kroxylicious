#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -ex
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/common.sh"
# Tmp fix - revert the nounset change made by common to match the expectations of this script.
set +u

RELEASE_DATE=$(date -u '+%Y-%m-%d')
REPOSITORY="origin"
BRANCH_FROM="main"
DRY_RUN="false"
ORIGINAL_GH_DEFAULT_REPO=""
while getopts ":v:b:u:dh" opt; do
  case $opt in
    v) RELEASE_VERSION="${OPTARG}"
    ;;
    b) RELEASE_DOCS_BRANCH="${OPTARG}"
    ;;
    u) WEBSITE_REPO_URL="${OPTARG}"
    ;;
    d) DRY_RUN="true"
    ;;
    h)
      1>&2 cat << EOF
usage: $0 -v version -u url -b branch [-r repository] [-d] [-h]
 -v version number e.g. 0.3.0
 -b name of the release branch to create in the website repository
 -u url of the website repository e.g. git@github.com:kroxylicious/kroxylicious.github.io.git
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

if [[ -z ${RELEASE_DOCS_BRANCH} ]]; then
  echo "No release branch specified, aborting"
  exit 1
fi

if [[ -z ${WEBSITE_REPO_URL} ]]; then
  echo "No website repository URL specified, aborting"
  exit 1
fi

if ! command -v gh &> /dev/null
then
    echo "gh command could not be found."
    exit 1
fi

BLUE='\033[0;34m'
NC='\033[0m' # No Color

ORIGINAL_WORKING_BRANCH=$(git branch --show-current)
ORIGINAL_WORKING_DIR=$(pwd)

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

    cd "${ORIGINAL_WORKING_DIR}"

    if [[ -n ${ORIGINAL_WORKING_BRANCH} ]]; then
        git checkout "${ORIGINAL_WORKING_BRANCH}" || true
    fi
}

trap cleanup EXIT

git stash --all

RELEASE_TAG="v${RELEASE_VERSION}"

WEBSITE_TMP=$(mktemp -d)

# Use a `/.` at the end of the source path to avoid the source path being appended to the destination path if the `.../_files/` folder already exists
KROXYLICIOUS_DOCS_LOCATION="${ORIGINAL_WORKING_DIR}/kroxylicious-docs/target/web"
WEBSITE_DOCS_LOCATION="${WEBSITE_TMP}/"

if [[ "${DRY_RUN:-false}" == true ]]; then
    #Disable the shell check as the colour codes only work with interpolation.
    # shellcheck disable=SC2059
    printf "${BLUE}Dry-run mode:${NC} no remote tags or PRs will be created, artefacts will be deployed to: ${DEPLOY_DRY_RUN_DIR}\n"
    GIT_DRYRUN="--dry-run"
fi

# GitHub sets at least one http...extraheader config option in Git which prevents using the Actions' SSH Key for other repos
# We have to find these settings and disable them here in the Kroxylicious repo before we can do anything with the website repo
# Otherwise commits and pushes wil fail
git config -l | grep 'http\..*\.extraheader' | cut -d= -f1 | xargs -L1 git config --unset-all

echo "Checking out tags/${RELEASE_TAG} in  in $(git remote get-url "${REPOSITORY}")"
git checkout "tags/${RELEASE_TAG}"

# Run docs build
mvn -P dist package --pl kroxylicious-docs

# Move to temp directory so we don't end up with website files in the main repository
cd "${WEBSITE_TMP}"
echo "In '$(pwd)', cloning website repository at ${WEBSITE_REPO_URL}"
git clone "${WEBSITE_REPO_URL}" "${WEBSITE_TMP}"

ORIGINAL_WEBSITE_WORKING_BRANCH=$(git branch --show-current)

echo "Creating branch ${RELEASE_DOCS_BRANCH} from ${BRANCH_FROM} in $(git remote get-url "${REPOSITORY}")"
git checkout -b "${RELEASE_DOCS_BRANCH}"

echo "Copying release docs from ${KROXYLICIOUS_DOCS_LOCATION} to ${WEBSITE_DOCS_LOCATION}"
cp -R "${KROXYLICIOUS_DOCS_LOCATION}"/* "${WEBSITE_DOCS_LOCATION}"

echo "Updating latest release to ${RELEASE_VERSION}"
${SED} -i -e "s/^latestRelease: .*$/latestRelease: ${RELEASE_VERSION}/g" _data/kroxylicious.yml

echo "Committing release documentation to git"
# Commit and push changes to branch in `kroxylicious/kroxylicious.github.io`
git add "${WEBSITE_DOCS_LOCATION}"
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
UNDERSCORED_VERSION=${RELEASE_VERSION//./_}
BODY="Prepare ${RELEASE_TAG} release documentation for publishing to website. Remember to replace the container image SHAs in \`_data/release/${UNDERSCORED_VERSION}.yaml\` once they are available, before merging!"
gh pr create --head "${RELEASE_DOCS_BRANCH}" \
             --base "${BRANCH_FROM}" \
             --title "Kroxylicious ${RELEASE_TAG} release documentation ${RELEASE_DATE}" \
             --body "${BODY}" \
             --repo "$(gh repo set-default -v)"
