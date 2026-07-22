#!/usr/bin/env bash
#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
set -euo pipefail

# Checks whether all changed files in a pull_request match a given pattern,
# writing only_matching_files_changed=true/false to GITHUB_OUTPUT.
# For all other event types the output is always false (workflow always runs).
#
# Usage: only-matching-files-changed.sh PATTERN
#
#   PATTERN  ERE matched against each changed filename.
#            only_matching_files_changed=true only when every changed file matches PATTERN.
#
# Required environment variables (set automatically by GitHub Actions):
#   GITHUB_OUTPUT       Path to the step-output file
#   GITHUB_EVENT_NAME   Event that triggered the workflow (pull_request / push / …)
#   GITHUB_BASE_REF     Base branch name (set automatically for pull_request events)

readonly PATTERN="${1:?PATTERN argument is required}"

# Change detection is only performed for pull_request events. For other events
# (push, workflow_dispatch, …) the workflow always runs. Limiting detection to
# pull requests means we can use git diff against the already-fetched base ref
# rather than calling the GitHub API, which avoids needing pull-requests: read
# permission in the token for push-triggered workflows.
if [[ "${GITHUB_EVENT_NAME}" != "pull_request" ]]; then
  echo "only_matching_files_changed=false" >> "${GITHUB_OUTPUT}"
  exit 0
fi

changed_files=$(git diff --name-only "origin/${GITHUB_BASE_REF}...HEAD")

if [[ -z "${changed_files}" ]]; then
  # Nothing changed - no reason to run
  echo "only_matching_files_changed=skip" >> "${GITHUB_OUTPUT}"
elif echo "${changed_files}" | grep -qvE "${PATTERN}"; then
  # At least one changed file does not match PATTERN - run the workflow
  echo "only_matching_files_changed=false" >> "${GITHUB_OUTPUT}"
else
  # Every changed file matches PATTERN - skip the workflow
  echo "only_matching_files_changed=true" >> "${GITHUB_OUTPUT}"
fi