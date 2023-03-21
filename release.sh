#!/usr/bin/env bash

#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

RELEASE_VERSION=${1:-0.1.0}
RELEASE_API_VERSION=${2:-0.1.0}
API_MODULES=':kroxylicious-api,:kroxylicious-filter-api'

echo "Validating the build is green"
mvn clean install || { echo 'maven build failed' ; exit 1; }
echo "Releasing API version ${RELEASE_API_VERSION} as part of version ${RELEASE_VERSION}"
mvn versions:set -DnewVersion="${RELEASE_API_VERSION}" -DprocessAllModules=true -pl ${API_MODULES} -DgenerateBackupPoms=false || { echo 'failed to set the API version' ; exit 1; }
mvn clean install -Pquick -pl ${API_MODULES}
mvn versions:set-property -Dproperty=kroxyliciousApi.version -DnewVersion="${RELEASE_API_VERSION}" -DgenerateBackupPoms=false || { echo "failed to depend on API version ${RELEASE_API_VERSION}" ; exit 1; }
mvn versions:set -DnewVersion="${RELEASE_VERSION}" -pl '!:kroxylicious-api,!:kroxylicious-filter-api'  -DgenerateBackupPoms=false || { echo 'failed to set the release version' ; exit 1; }
echo "Validating things still build"
mvn clean install -Pquick

echo "Committing release to git"
git add **/pom.xml && git commit -m "Release version v${RELEASE_VERSION}" && git tag "api-v${RELEASE_API_VERSION}"