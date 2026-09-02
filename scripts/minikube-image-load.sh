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
# Usage: minikube-image-load.sh <profile> <tarball> <expected-repo>

set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <profile> <tarball> <expected-repo>" >&2
  exit 2
fi

profile=$1
tarball=$2
expected_repo=$3

# Check that the tarball exists before attempting to load it
if [[ ! -f "${tarball}" ]]; then
  echo "minikube-image-load: tarball does not exist: ${tarball}" >&2
  exit 1
fi

# Load the image into the specified Minikube profile
minikube image load -p "${profile}" "${tarball}"

# Verify that the expected repository is now listed in the profile's images
if ! minikube image ls -p "${profile}" | grep -qF "${expected_repo}"; then
  echo "minikube-image-load: ${expected_repo} is not in profile '${profile}' after loading ${tarball} (kubernetes/minikube#23471)" >&2
  exit 1
fi

# Print the list of images in the profile that match the expected repository
minikube image ls -p "${profile}" | grep -F "${expected_repo}"
