#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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

readonly PATTERN="${1:?PATTERN argument is required}"

# Change detection is only performed for pull_request events. For other events
# (push, workflow_dispatch, …) the workflow always runs. Limiting detection to
# pull requests means we can use git diff against HEAD^1 (the base branch tip,
# which is the first parent of the synthetic merge commit that actions/checkout
# creates for PRs) rather than calling the GitHub API, which avoids needing
# pull-requests: read permission in the token for push-triggered workflows.
if [[ "${GITHUB_EVENT_NAME}" != "pull_request" ]]; then
  echo "only_matching_files_changed=false" >> "${GITHUB_OUTPUT}"
  exit 0
fi

changed_files=$(git diff --name-only "HEAD^1...HEAD")

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