#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

usage() {
      1>&2 cat << EOF
usage: $0 <-a|-s (close|drop|release)|-h>
 -a asserts that no staging repos exist
 -s desired state to move the staging repository (close|drop|release)
 -h this help message
EOF
exit 1
}

PLUGIN=org.sonatype.plugins:nexus-staging-maven-plugin:1.6.13
MVN_ARGS=("--batch-mode" "--no-transfer-progress" "-DnexusUrl=https://s01.oss.sonatype.org/" "-DserverId=ossrh")

ASSERT_NO_STAGING_REPOS="false"
STATE=""
while getopts ":s:ah" opt; do
  case $opt in
    a) ASSERT_NO_STAGING_REPOS="true"
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

# TODO: refactor to use the REST API https://oss.sonatype.org/nexus-staging-plugin/default/docs/index.html to avoid the awkish hell
# curl --silent -u xxxx:xxx  -H "accept: application/json" -X GET https://s01.oss.sonatype.org/service/local/staging/profile_repositories | jq .


# The rc-list table looks like this:
#[INFO] ID                   State    Description
#[INFO] iokroxylicious-1032  CLOSED   unknown
#[INFO] ------------------------------------------------------------------------

STAGING_REPO_IDS=$(mvn ${PLUGIN}:rc-list "${MVN_ARGS[@]}" 2>/dev/null \
                 | awk '/\[INFO\] ---*/{found=0} found==1{print $2} /\[INFO\] ID.*State.*Description/{found=1}')

NUM_REPOS=$(echo "${STAGING_REPO_IDS}" | (grep -c . || true))

if [[ ${ASSERT_NO_STAGING_REPOS} == "true" ]]; then
  if [[ ${NUM_REPOS} -gt 0 ]]; then
      >&2 echo "${NUM_REPOS} stage repository(s) already exists but expected none."
     exit ${NUM_REPOS}
  fi
  exit 0
elif [[ ${STATE} ]]; then
  if [[ ${NUM_REPOS} -eq 0 ]]; then
      >&2 echo "No staging repository found to apply a ${STATE} state transition."
     if [[ ${STATE} == "drop" ]]; then
         exit 0
     fi
     exit 1
  elif [[ ${NUM_REPOS} -gt 1 ]]; then
      >&2 echo "Too many staging repositories found (${NUM_REPOS})."
     exit 1
  fi

  case ${STATE} in
    close|drop|release)
      # Workaround for https://issues.sonatype.org/browse/OSSRH-66257
      MAVEN_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.desktop/java.awt.font=ALL-UNNAMED" \
        mvn ${PLUGIN}:${STATE} "${MVN_ARGS[@]}" -DstagingRepositoryId=${STAGING_REPO_IDS} -DstagingProgressTimeoutMinutes=20
      ;;
    *)
      usage
      ;;
  esac
else
  usage
fi
