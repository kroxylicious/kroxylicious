#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# Set up SED for platform compatibility (gsed on macOS, sed on Linux)
OS=$(uname)
if [ "$OS" = 'Darwin' ]; then
  SED=$(command -v gsed || echo "sed")
else
  SED=$(command -v sed)
fi

# Default chunk size
TESTS_PER_CHUNK=${1:-15}

# Discover ITs from all modules, format as "module:TestClass"
# Exclude target dirs and archetype template resources
TEST_LIST=$(find . -type f -name "*IT.java" \
  -not -path "*/target/*" \
  -not -path "*/archetype-resources/*" \
  | while read -r file; do
      testname=$(basename "$file" .java)
      # Extract module path: ./kroxylicious-operator/src/... -> kroxylicious-operator
      module=$(echo "$file" | ${SED} 's|^\./||' | ${SED} 's|/src/.*||')
      echo "${module}:${testname}"
    done | sort)

# Group tests by module, shuffle within each module, then chunk
# Compatible with bash 3.2+ (no associative arrays)
CHUNKS=()
current_module=""
module_tests=""

while IFS= read -r spec; do
    if [ -z "$spec" ]; then
        continue
    fi

    module=$(echo "$spec" | cut -d: -f1)
    test=$(echo "$spec" | cut -d: -f2)

    # If we hit a new module, process the previous module's tests
    if [ -n "$current_module" ] && [ "$module" != "$current_module" ]; then
        # Shuffle and chunk the accumulated tests
        shuffled=$(echo "$module_tests" | tr ' ' '\n' | grep -v '^$' | shuf)
        while IFS= read -r chunk; do
            if [ -n "$chunk" ]; then
                # Prefix each test with module: and convert spaces to commas
                prefixed=$(echo "$chunk" | ${SED} "s|\([^ ]*\)|${current_module}:\1|g" | tr ' ' ',')
                CHUNKS+=("\"$prefixed\"")
            fi
        done <<< "$(echo "$shuffled" | xargs -L ${TESTS_PER_CHUNK})"

        # Reset for new module
        module_tests=""
    fi

    current_module="$module"
    module_tests="${module_tests}${test} "
done <<< "$TEST_LIST"

# Process the last module
if [ -n "$current_module" ] && [ -n "$module_tests" ]; then
    shuffled=$(echo "$module_tests" | tr ' ' '\n' | grep -v '^$' | shuf)
    while IFS= read -r chunk; do
        if [ -n "$chunk" ]; then
            # Prefix each test with module: and convert spaces to commas
            prefixed=$(echo "$chunk" | ${SED} "s|\([^ ]*\)|${current_module}:\1|g" | tr ' ' ',')
            CHUNKS+=("\"$prefixed\"")
        fi
    done <<< "$(echo "$shuffled" | xargs -L ${TESTS_PER_CHUNK})"
fi

# Output JSON array
TEST_CHUNKS="[$(IFS=,; echo "${CHUNKS[*]}")]"
echo "$TEST_CHUNKS"
