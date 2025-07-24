#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

usage() {
      1>&2 cat << "EOF"
usage: $0 -d <distribution-id> -s (drop|release)|-h
 -d Central Publishing Portal distribution id to act upon
 -s desired state to move the staging repository (drop|release)
 -h this help message
EOF
exit 1
}


STATE=""
DEPLOYMENT_ID=""
while getopts ":d:s:h" opt; do
  case $opt in
    s) STATE="${OPTARG}"
    ;;
    d) DEPLOYMENT_ID="${OPTARG}"
    ;;
    h) usage
    ;;
    \?) echo "Invalid option -${OPTARG}" >&2
    exit 1
    ;;
  esac
done

if [[ -z "${STATE}" ]]; then
    echo "No next state specified. Please specify -s drop|release" 1>&2
    exit 1
fi

if [[ -z "${DEPLOYMENT_ID}" ]]; then
    echo "No deployment id specified. Please specify -d <central publishing portal distribution id>" 1>&2
    exit 1
fi

AUTH=$(printf "${SONATYPE_TOKEN_USERNAME}:${SONATYPE_TOKEN_PASSWORD}" | base64)

# https://central.sonatype.org/publish/publish-portal-api/#publish-or-drop-the-deployment
case ${STATE} in
  drop)
      echo Dropping ${DEPLOYMENT_ID}
      curl --request DELETE \
           --header "Authorization: Bearer ${AUTH}" \
           https://central.sonatype.com/api/v1/publisher/deployment/${DEPLOYMENT_ID}
      ;;
  release)
      # Publishing ${DEPLOYMENT_ID}
      curl --request POST \
           --header "Authorization: Bearer ${AUTH}" \
           https://central.sonatype.com/api/v1/publisher/deployment/${DEPLOYMENT_ID}
      ;;
  *)
    usage
    ;;
esac
