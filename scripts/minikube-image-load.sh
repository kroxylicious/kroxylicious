#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Loads a container-image tarball into a Minikube profile and *fails* if it did not actually land.
#
# `minikube image load` exits 0 even when the load failed (kubernetes/minikube#23471) - a missing
# tarball, for example, only prints "The image ... was not found" and still exits 0. So this wrapper:
#   1. fails up front if the tarball does not exist;
#   2. after the load, fails unless `minikube image ls` lists <expected-repo> (e.g. kroxylicious/operator).
#
# Usage: minikube-image-load.sh [--profile|-p <profile>] <tarball> <expected-repo>

set -euo pipefail

profile=""
while getopts "p:-:" opt; do
  case $opt in
    p)
      profile="$OPTARG"
      ;;
    -)
      case "$OPTARG" in
        profile)
          profile="${!OPTIND}"
          OPTIND=$((OPTIND + 1))
          ;;
        profile=*)
          profile="${OPTARG#*=}"
          ;;
        *)
          echo "usage: $0 [--profile|-p <profile>] <tarball> <expected-repo>" >&2
          exit 2
          ;;
      esac
      ;;
    *)
      echo "usage: $0 [--profile|-p <profile>] <tarball> <expected-repo>" >&2
      exit 2
      ;;
  esac
done
shift $((OPTIND - 1))

if [[ $# -ne 2 ]]; then
  echo "usage: $0 [--profile|-p <profile>] <tarball> <expected-repo>" >&2
  exit 2
fi

tarball=$1
expected_repo=$2

# Check that the tarball exists before attempting to load it
if [[ ! -f "${tarball}" ]]; then
  echo "minikube-image-load: tarball does not exist: ${tarball}" >&2
  exit 1
fi

# Load the image into the specified Minikube profile (or current profile if not specified)
if [[ -n "${profile}" ]]; then
  minikube image load -p "${profile}" "${tarball}"
else
  minikube image load "${tarball}"
fi

# Verify that the expected repository is now listed in the profile's images
if [[ -n "${profile}" ]]; then
  image_list=$(minikube image ls -p "${profile}")
else
  image_list=$(minikube image ls)
fi

if ! grep -qF "${expected_repo}" <<< "${image_list}"; then
  if [[ -n "${profile}" ]]; then
    echo "minikube-image-load: ${expected_repo} is not in profile '${profile}' after loading ${tarball} (kubernetes/minikube#23471)" >&2
  else
    echo "minikube-image-load: ${expected_repo} is not in current profile after loading ${tarball} (kubernetes/minikube#23471)" >&2
  fi
  exit 1
fi

# Print the list of images in the profile that match the expected repository
grep -F "${expected_repo}" <<< "${image_list}"
