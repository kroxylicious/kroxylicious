#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
# Compares the legacy root-Dockerfile image build against the Maven-artifact-based
# build introduced in https://github.com/kroxylicious/kroxylicious/pull/3553.
#
# Requires: docker (with buildx) or podman, syft, jq, mvn
#
# Usage:
#   ./scripts/compare-image-builds.sh               # compares proxy images
#   ./scripts/compare-image-builds.sh -o            # compares operator images
#   ./scripts/compare-image-builds.sh -p -o         # compares both
#   CONTAINER_ENGINE=podman ./scripts/compare-image-builds.sh  # use podman

set -eo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "${SCRIPT_DIR}/.."

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NO_COLOUR='\033[0m'

COMPARE_PROXY=false
COMPARE_OPERATOR=false

usage() {
  cat <<EOF
Usage: $(basename "$0") [-p] [-o] [-h]
  -p  Compare proxy images (default if neither -p nor -o given)
  -o  Compare operator images
  -h  This help message
EOF
  exit 1
}

while getopts ":poh" opt; do
  case $opt in
    p) COMPARE_PROXY=true ;;
    o) COMPARE_OPERATOR=true ;;
    h) usage ;;
    \?) echo "Invalid option: -${OPTARG}" >&2; usage ;;
  esac
done

# Default to proxy if nothing specified
if [[ "${COMPARE_PROXY}" == false && "${COMPARE_OPERATOR}" == false ]]; then
  COMPARE_PROXY=true
fi

CONTAINER_ENGINE="${CONTAINER_ENGINE:-docker}"

# Check required tools
for tool in "${CONTAINER_ENGINE}" syft jq mvn; do
  if ! command -v "${tool}" &>/dev/null; then
    echo -e "${RED}Required tool not found: ${tool}${NO_COLOUR}" >&2
    exit 1
  fi
done

if [[ "${CONTAINER_ENGINE}" == "docker" ]] && ! docker buildx version &>/dev/null; then
  echo -e "${RED}docker buildx is required but not available${NO_COLOUR}" >&2
  exit 1
fi

VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo -e "${GREEN}Kroxylicious version: ${VERSION}${NO_COLOUR}"

# -----------------------------------------------------------------------
# Build Maven distribution artifacts (needed by the new-style Dockerfiles)
# -----------------------------------------------------------------------
echo -e "\n${YELLOW}Building Maven distribution artifacts...${NO_COLOUR}"
mvn --quiet --batch-mode --activate-profiles dist \
  --also-make --projects :kroxylicious-app,:kroxylicious-operator,:kroxylicious-operator-dist \
  -Dquick -DskipContainerImageBuild=true package

compare_image() {
  local name=$1           # e.g. "proxy" or "operator"
  local old_dockerfile=$2 # e.g. "Dockerfile"
  local old_context=$3    # e.g. "."
  local new_dockerfile=$4 # e.g. "kroxylicious-app/src/main/docker/proxy.dockerfile"
  local new_context=$5    # e.g. "kroxylicious-app"
  local start_cmd=$6      # command to test startup

  local old_tag="kroxylicious-${name}:old-dockerfile"
  local new_tag="kroxylicious-${name}:new-dockerfile"

  echo -e "\n${YELLOW}=== ${name} image comparison ===${NO_COLOUR}"

  echo -e "\n${YELLOW}Building old ${name} image (root Dockerfile)...${NO_COLOUR}"
  "${CONTAINER_ENGINE}" build -t "${old_tag}" \
    --build-arg KROXYLICIOUS_VERSION="${VERSION}" \
    -f "${old_dockerfile}" \
    "${old_context}"

  echo -e "\n${YELLOW}Building new ${name} image (Maven artifacts + module Dockerfile)...${NO_COLOUR}"
  if [[ "${CONTAINER_ENGINE}" == "docker" ]]; then
    docker buildx build -t "${new_tag}" --load \
      --build-arg KROXYLICIOUS_VERSION="${VERSION}" \
      -f "${new_dockerfile}" \
      "${new_context}"
  else
    "${CONTAINER_ENGINE}" build -t "${new_tag}" \
      --build-arg KROXYLICIOUS_VERSION="${VERSION}" \
      -f "${new_dockerfile}" \
      "${new_context}"
  fi

  echo -e "\n${YELLOW}--- SBOM diff (packages) ---${NO_COLOUR}"
  local old_sbom new_sbom
  local old_tar new_tar
  old_tar=$(mktemp).tar
  new_tar=$(mktemp).tar
  "${CONTAINER_ENGINE}" save -o "${old_tar}" "${old_tag}"
  "${CONTAINER_ENGINE}" save -o "${new_tar}" "${new_tag}"

  old_sbom=$(mktemp)
  new_sbom=$(mktemp)
  syft "docker-archive:${old_tar}" -o syft-json 2>/dev/null | jq -r '.artifacts[].name' | sort > "${old_sbom}" || { echo -e "${RED}syft failed for old image${NO_COLOUR}" >&2; rm -f "${old_tar}" "${new_tar}"; return 1; }
  syft "docker-archive:${new_tar}" -o syft-json 2>/dev/null | jq -r '.artifacts[].name' | sort > "${new_sbom}" || { echo -e "${RED}syft failed for new image${NO_COLOUR}" >&2; rm -f "${old_tar}" "${new_tar}"; return 1; }
  rm -f "${old_tar}" "${new_tar}"
  if diff "${old_sbom}" "${new_sbom}"; then
    echo -e "${GREEN}No package differences${NO_COLOUR}"
  fi
  rm -f "${old_sbom}" "${new_sbom}"

  echo -e "\n${YELLOW}--- Metadata diff (entrypoint, user, env, labels) ---${NO_COLOUR}"
  local old_meta new_meta
  old_meta=$(mktemp)
  new_meta=$(mktemp)
  "${CONTAINER_ENGINE}" inspect "${old_tag}" | jq '.[0].Config | {Entrypoint, Cmd, User, Env, Labels, WorkingDir}' > "${old_meta}"
  "${CONTAINER_ENGINE}" inspect "${new_tag}" | jq '.[0].Config | {Entrypoint, Cmd, User, Env, Labels, WorkingDir}' > "${new_meta}"
  if diff "${old_meta}" "${new_meta}"; then
    echo -e "${GREEN}No metadata differences${NO_COLOUR}"
  fi
  rm -f "${old_meta}" "${new_meta}"

  echo -e "\n${YELLOW}--- Startup sanity check (new image) ---${NO_COLOUR}"
  # shellcheck disable=SC2086
  if "${CONTAINER_ENGINE}" run --rm "${new_tag}" ${start_cmd}; then
    echo -e "${GREEN}Startup check passed${NO_COLOUR}"
  else
    echo -e "${RED}Startup check failed${NO_COLOUR}"
    return 1
  fi
}

if [[ "${COMPARE_PROXY}" == true ]]; then
  compare_image \
    "proxy" \
    "Dockerfile" \
    "." \
    "kroxylicious-app/src/main/docker/proxy.dockerfile" \
    "kroxylicious-app" \
    "/opt/kroxylicious/bin/kroxylicious-start.sh --help"
fi

if [[ "${COMPARE_OPERATOR}" == true ]]; then
  compare_image \
    "operator" \
    "Dockerfile.operator" \
    "." \
    "kroxylicious-operator/src/main/docker/operator.dockerfile" \
    "kroxylicious-operator" \
    "/opt/kroxylicious-operator/bin/operator-start.sh --help"
fi

echo -e "\n${GREEN}Done.${NO_COLOUR}"
