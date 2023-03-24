#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -o nounset -e

RELEASE_API_VERSION=${1}
API_MODULES=':kroxylicious-api,:kroxylicious-filter-api'

if [[ -z "${GPG_KEY}" ]]; then
    echo "GPG_KEY not set unable to sign the release. Please export GPG_KEY" 1>&2
    exit 1
fi

if [[ -z ${RELEASE_API_VERSION} ]]; then
  echo "no api release version specified please specify at least one"
  exit 1
fi

echo "Setting API version to ${RELEASE_API_VERSION}"
mvn -q versions:set -DnewVersion="${RELEASE_API_VERSION}" -DprocessAllModules=true -pl ":kroxylicious-api" -DgenerateBackupPoms=false
mvn -q versions:set -DnewVersion="${RELEASE_API_VERSION}" -DprocessAllModules=true -pl ":kroxylicious-filter-api" -DgenerateBackupPoms=false
mvn -q clean install -Pquick -pl ${API_MODULES}  #quick sanity check to ensure the API modules still build
mvn -q versions:set-property -Dproperty=kroxyliciousApi.version -DnewVersion="${RELEASE_API_VERSION}" -DgenerateBackupPoms=false

echo "Validating things still build"
mvn -q clean install -Pquick

echo "Committing API release to git"
git add '**/pom.xml' 'pom.xml'
git commit --message "Release API version v${RELEASE_API_VERSION}" --signoff

git tag -f "api-v${RELEASE_API_VERSION}"

#echo "Deploying release to maven central"
#mvn deploy -Prelease -DskipTests=true -DreleaseSigningKey="${GPG_KEY}" -pl "${API_MODULES}"
