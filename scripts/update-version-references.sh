#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Updates all Maven and non-Maven version references to a new version.
# Intended to be called from the project root by release scripts.
#
# Usage: update-version-references.sh -v <version> -g <git-ref> [-a <api-compat-reference-version>]
#  -v  version to apply (e.g. 0.25.1-SNAPSHOT or 0.25.0)
#  -g  value for :KroxyliciousGitRef: in attributes.adoc (e.g. release/v0.25 or v0.25.0)
#  -a  if specified, sets ApiCompatability.ReferenceVersion to this value and re-enables enforcement

set -e
set -o pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/common.sh"
set +u

while getopts ":v:g:a:h" opt; do
  case $opt in
    v) VERSION="${OPTARG}"
    ;;
    g) GIT_REF="${OPTARG}"
    ;;
    a) API_COMPAT_REFERENCE_VERSION="${OPTARG}"
    ;;
    h)
      1>&2 cat << EOF
usage: $0 -v version -g git-ref [-a api-compat-reference-version] [-h]
 -v  version to apply (e.g. 0.25.1-SNAPSHOT or 0.25.0)
 -g  value for :KroxyliciousGitRef: in attributes.adoc (e.g. release/v0.25 or v0.25.0)
 -a  if specified, sets ApiCompatability.ReferenceVersion to this value and re-enables enforcement
 -h  this help message
EOF
      exit 1
    ;;
    \?) echo "Invalid option -$opt ${OPTARG}" >&2
    exit 1
    ;;
  esac
done

if [[ -z "${VERSION}" ]]; then
    echo "No version specified. Please specify -v <version>" 1>&2
    exit 1
fi

if [[ -z "${GIT_REF}" ]]; then
    echo "No git ref specified. Please specify -g <git-ref>" 1>&2
    exit 1
fi

mvn --quiet --batch-mode versions:set -DnewVersion="${VERSION}" -DgenerateBackupPoms=false -DprocessAllModules=true
git add '**/*.yaml' '**/pom.xml' 'pom.xml'

replaceInFile "s_:KroxyliciousVersion:.*_:KroxyliciousVersion: ${VERSION}_g" \
  kroxylicious-docs/docs/_assets/attributes.adoc
replaceInFile "s_:KroxyliciousGitRef:.*_:KroxyliciousGitRef: ${GIT_REF}_g" \
  kroxylicious-docs/docs/_assets/attributes.adoc

replaceInFile "s_image: 'quay.io/kroxylicious/proxy:.*'_image: 'quay.io/kroxylicious/proxy:${VERSION}'_g" \
  compose/kafka-compose.yaml

replaceInFile "s|KROXYLICIOUS_VERSION:-[^}]*|KROXYLICIOUS_VERSION:-${VERSION}|g" \
  kroxylicious-openmessaging-benchmarks/scripts/setup-cluster.sh

replaceInFile "s_quay\.io/kroxylicious/proxy:[^}]*_quay.io/kroxylicious/proxy:${VERSION}_g" \
  performance-tests/docker-compose.yaml

replaceInFile "s_io\.kroxylicious:kroxylicious-migrations:[^ ]*_io.kroxylicious:kroxylicious-migrations:${VERSION}_g" \
  kroxylicious-proxy-core/kroxylicious-migrations/README.md

if [[ -n "${API_COMPAT_REFERENCE_VERSION}" ]]; then
  mvn --quiet --batch-mode --projects :kroxylicious-api versions:set-property \
    -Dproperty="ApiCompatability.ReferenceVersion" -DnewVersion="${API_COMPAT_REFERENCE_VERSION}" \
    -DgenerateBackupPoms=false
  mvn --quiet --batch-mode --projects :kroxylicious-api versions:set-property \
    -Dproperty="ApiCompatability.EnforceForMajorVersionZero" -DnewVersion="true" \
    -DgenerateBackupPoms=false
  git add kroxylicious-api/pom.xml
fi
