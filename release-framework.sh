#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -o nounset -e

RELEASE_VERSION=${1}

if [[ -z "${GPG_KEY}" ]]; then
    echo "GPG_KEY not set unable to sign the release. Please export GPG_KEY" 1>&2
    exit 1
fi

if [[ -z ${RELEASE_VERSION} ]]; then
  echo "no api release version specified please specify at least one"
  exit 1
fi

echo "Validating the build is green"
mvn clean verify || { echo 'maven build failed' ; exit 1; }

mvn versions:set -DnewVersion="${RELEASE_VERSION}" -DgenerateBackupPoms=false || { echo 'failed to set the release version' ; exit 1; }
echo "Validating things still build"
mvn clean install -Pquick

echo "Committing framework release to git"
git add '**/pom.xml' 'pom.xml'
git commit --message "Release Framework version v${RELEASE_VERSION}" --signoff

git tag -f "v${RELEASE_VERSION}"

#echo "Deploying release to maven central"
#mvn deploy -Prelease -DskipTests=true -DreleaseSigningKey="${GPG_KEY}"
