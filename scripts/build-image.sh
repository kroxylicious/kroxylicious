#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/common.sh"
cd "${SCRIPT_DIR}/.."
KROXYLICIOUS_VERSION=${KROXYLICIOUS_VERSION:-$(mvn org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=project.version -q -DforceStdout)}
LABELS=()
IMAGE_TAGS=()

function array_to_arg_line() {
  local ARG_NAME=$1
  shift
  local ARR=("$@")
  local arg_line=
  if [ ${#ARR[@]} -gt 0 ]; then
    for value in "${ARR[@]}"; do
      arg_line+="--${ARG_NAME} ${value}"
    done
  fi
  echo "${arg_line}"
}

while getopts ":l:t:s:h" opt; do
  case $opt in
    l) LABELS+=("${OPTARG}")
    ;;
    t) IMAGE_TAGS+=("${OPTARG}")
    ;;
    s) IMAGE_EXPIRY="${OPTARG}"
    ;;
    h)
      1>&2 cat << EOF
usage: $0 [-l <label>] [-t tag>] [-h] [-s <expiry string>]
 -l a label to add to the image
 -t a tag to add to the image
 -s short term image aka a temporary image. e.g. '8h'. Use 'h' for hours, 'd' for days and 'w' for weeks. See https://docs.projectquay.io/use_quay.html#setting-tag-from-dockerfile
 -h this help message
EOF
      exit 1
    ;;
    \?) echo "Invalid option -$opt ${OPTARG}" >&2
    exit 1
    ;;
  esac

done


if [[ -z ${REGISTRY_DESTINATION:-} ]]; then
  echo "Please set REGISTRY_DESTINATION to a value like 'quay.io/<myorg>/kroxylicious', exiting" 1>&2
  exit 1
fi

IMAGE="${REGISTRY_DESTINATION}:${KROXYLICIOUS_VERSION}"
CONTAINERFILE="${CONTAINERFILE:-Dockerfile}"

if [ -n "${IMAGE_EXPIRY:-}" ]; then
  LABELS+=("quay.expires-after=${IMAGE_EXPIRY}")
fi

LABEL_ARGS=$(array_to_arg_line "label" "${LABELS[@]:""}")
TAG_ARGS=$(array_to_arg_line "tag" "${IMAGE_TAGS[@]:""}")

# shellcheck disable=SC2086 #we are passing additional arguments here so word splitting is intended
${CONTAINER_ENGINE} build -f "${CONTAINERFILE}" -t "${IMAGE}" ${LABEL_ARGS} ${TAG_ARGS} \
                                        --build-arg "KROXYLICIOUS_VERSION=${KROXYLICIOUS_VERSION}" \
                                        --build-arg "CONTAINER_USER=${USER}" \
                                        --build-arg "CONTAINER_USER_UID=$(id -u)" \
                                        .
if [[ -n ${PUSH_IMAGE:-} ]]; then
  REGISTRY_SERVER=${REGISTRY_SERVER:-$(extractRegistryServer "${REGISTRY_DESTINATION}")}
  echo "Pushing image to ${REGISTRY_SERVER}"
  ${CONTAINER_ENGINE} login ${REGISTRY_SERVER}""
  ${CONTAINER_ENGINE} push "${IMAGE}"
  if [ ${#IMAGE_TAGS[@]} -gt 0 ]; then
      for tag in "${IMAGE_TAGS[@]}"; do
        ${CONTAINER_ENGINE} tag "${IMAGE}" "${REGISTRY_DESTINATION}:${tag}"
        ${CONTAINER_ENGINE} push "${REGISTRY_DESTINATION}:${tag}"
      done
  fi
else
  echo "PUSH_IMAGE not set, not pushing to container registry"
fi
