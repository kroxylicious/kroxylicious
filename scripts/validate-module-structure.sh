#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Validates the Maven module-profile structure in the root pom.xml.
#
# Rules:
#   1. The root <modules> block must have no direct <module> children.
#      New modules belong in an intermediate parent pom.xml.
#   2. Every module directory listed in the build-the-world profile must
#      exist and contain a non-empty intermediate parent pom.xml.
#
# Usage: validate-module-structure.sh [path/to/pom.xml]
#        Defaults to pom.xml in the current directory.

set -euo pipefail
set -x

POM="${1:-pom.xml}"
PROJECT_DIR="$(cd "$(dirname "$POM")" && pwd)"
ERRORS=0

xmllint --version 2>&1 || true

fail() {
    echo "ERROR: $*" >&2
    ERRORS=$((ERRORS + 1))
}

xpath_count() {
    xmllint --xpath "count($1)" "$2" 2>&1 || echo 0
}

xpath_text_lines() {
    xmllint --xpath "$1" "$2" 2>&1 || true
}

# Rule 1: root <modules> must have no direct children.
check_root_modules_empty() {
    local count
    count=$(xpath_count \
        "/*[local-name()='project']/*[local-name()='modules']/*[local-name()='module']" \
        "$POM")
    if [ "$count" -ne 0 ]; then
        fail "Root pom.xml <modules> has $count direct $([ "$count" -eq 1 ] && echo entry || echo entries)." \
             "Add new modules to an intermediate parent pom.xml instead."
    fi
}

# Rule 2: every module listed in build-the-world must have a non-empty pom.xml.
check_build_the_world_parents() {
    local modules
    modules=$(xpath_text_lines \
        "//*[local-name()='profile'][*[local-name()='id' and text()='build-the-world']]/*[local-name()='modules']/*[local-name()='module']/text()" \
        "$POM")

    if [ -z "$modules" ]; then
        fail "build-the-world profile has no modules."
        return
    fi

    local module_dir parent_pom leaf_count
    while IFS= read -r module_dir; do
        [ -n "$module_dir" ] || continue
        parent_pom="$PROJECT_DIR/$module_dir/pom.xml"

        if [ ! -f "$parent_pom" ]; then
            fail "'$module_dir' is in build-the-world but $parent_pom does not exist."
            continue
        fi

        leaf_count=$(xpath_count \
            "/*[local-name()='project']/*[local-name()='modules']/*[local-name()='module']" \
            "$parent_pom")
        if [ "$leaf_count" -eq 0 ]; then
            fail "$parent_pom has no <module> entries — intermediate parents must not be empty."
        fi
    done <<< "$modules"
}

check_root_modules_empty
check_build_the_world_parents

if [ "$ERRORS" -eq 0 ]; then
    echo "Module-profile structure OK"
else
    exit 1
fi
