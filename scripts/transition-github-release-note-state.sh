#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#


set -euo pipefail

usage() {
      1>&2 cat << EOF
usage: $0 -v <release number> <-a|-s (close|drop|release)|-h>
 -v version number e.g. 0.3.0
 -a asserts that no release notes exist for specified version
 -s desired state (release to undraft the release note, drop to delete it)
 -h this help message
EOF
exit 1
}

NAME_WITH_OWNER=$(gh repo view --json nameWithOwner --template '{{ .nameWithOwner }}')
GH_AUTH_TOKEN=$(gh auth token)
CURL_ARGS=("--silent" "--header" "Accept: application/vnd.github+json" "--header" "Authorization: Bearer ${GH_AUTH_TOKEN}" "--header" "X-GitHub-Api-Version: 2022-11-28" "https://api.github.com/repos/${NAME_WITH_OWNER}/releases")

ASSERT_NO_RELEASE_NOTES_EXIST="false"
STATE=""
while getopts ":s:v:ah" opt; do
  case $opt in
    v) RELEASE_VERSION="${OPTARG}"
    ;;
    a) ASSERT_NO_RELEASE_NOTES_EXIST="true"
    ;;
    s) STATE="${OPTARG}"
    ;;
    h) usage
    ;;
    \?) echo "Invalid option -${OPTARG}" >&2
    exit 1
    ;;
  esac
done

if [[ -z ${RELEASE_VERSION} ]]; then
  echo "No version specified, aborting"
  exit 1
fi


TAG=v${RELEASE_VERSION}
if [[ ${ASSERT_NO_RELEASE_NOTES_EXIST} == "true" ]]; then
  NUM_EXIST_RELEASE_NOTES=$(curl "${CURL_ARGS[@]}" | jq --arg tag "${TAG}" '[.[] | select ( .tag_name == $tag )] | length')
  if [[ ${NUM_EXIST_RELEASE_NOTES} -gt 0 ]]; then
      >&2 echo "${NUM_EXIST_RELEASE_NOTES} release note(s) already exist for tag ${TAG}"
   exit 11
  fi
  exit 0
elif [[ ${STATE} ]]; then
  NUM_EXIST_DRAFT_RELEASE_NOTES=$(curl "${CURL_ARGS[@]}" | jq --arg tag "${TAG}" '[.[] | select ( .tag_name == $tag and .draft == true )] | length')
  if [[ ${NUM_EXIST_DRAFT_RELEASE_NOTES} == 0 && ${STATE} == "drop" ]]; then
    >&2 echo "No draft release notes for tag ${TAG}."
    exit 0
  fi

  if [[ ${NUM_EXIST_DRAFT_RELEASE_NOTES} != 1 ]]; then
      >&2 echo "Unexpected number of draft release notes for tag ${TAG} found (${NUM_EXIST_DRAFT_RELEASE_NOTES})"
   exit 11
  fi

  if [[ ${STATE} == "release" ]]; then
    gh release edit --verify-tag --draft=false "${TAG}"
  else
    gh release delete --cleanup-tag --yes "${TAG}"
  fi
else
  usage
fi
