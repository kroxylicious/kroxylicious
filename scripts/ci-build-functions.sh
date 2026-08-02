#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Shared constants and functions for CI workflows.
# Source this file to access the variables and functions below.

# Maven profile names for module groups.
# When a new group is introduced, add a variable here and update the
# root pom.xml profile list.
readonly PROXY_MODULES="proxy-runtime,runtime-plugins,supplementary"
readonly KUBERNETES_MODULES="kubernetes-management"

# Run a Sonar scan for a module group on a pull request.
#
# Usage: run_sonar <modules> <project_name> <project_key> [extra mvn args...]
#
# <modules>      Comma-separated Maven profile names for the module group
#                (use PROXY_MODULES or KUBERNETES_MODULES from above).
# <project_name> sonar.projectName value (e.g. 'Proxy Runtime')
# <project_key>  sonar.projectKey value  (e.g. 'kroxylicious_kroxylicious')
# [extra args]   Passed directly to mvn — use for -Djapicmp.skip,
#                -Dsonar.pullrequest.*, -Dsonar.scm.revision, etc.
run_sonar() {
  local modules="$1"
  local project_name="$2"
  local project_key="$3"
  shift 3
  mvn --batch-mode \
    -P "!build-the-world,!qa" \
    -P "${modules},ci" \
    -Dsonar.projectName="${project_name}" \
    -Dsonar.projectKey="${project_key}" \
    -Derrorprone.skip=true \
    -DskipITs=true \
    "$@" \
    clean verify \
    org.sonarsource.scanner.maven:sonar-maven-plugin:5.5.0.6356:sonar
}
