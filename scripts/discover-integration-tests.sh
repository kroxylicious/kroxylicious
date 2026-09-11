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

# Function to discover and chunk tests by pattern
# $1 = file name pattern (e.g., "*IT.java" or "*KT.java")
discover_and_chunk() {
  local pattern="$1"
  local chunks=()

  # Discover tests matching the pattern, format as "module:TestClass"
  # Exclude target dirs and archetype template resources
  local test_list=$(find . -type f -name "$pattern" \
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
  local current_module=""
  local module_tests=""

  while IFS= read -r spec; do
      if [ -z "$spec" ]; then
          continue
      fi

      local module=$(echo "$spec" | cut -d: -f1)
      local test=$(echo "$spec" | cut -d: -f2)

      # If we hit a new module, process the previous module's tests
      if [ -n "$current_module" ] && [ "$module" != "$current_module" ]; then
          # Shuffle and chunk the accumulated tests
          local shuffled=$(echo "$module_tests" | tr ' ' '\n' | grep -v '^$' | shuf)
          while IFS= read -r chunk; do
              if [ -n "$chunk" ]; then
                  # Prefix each test with module: and convert spaces to commas
                  local prefixed=$(echo "$chunk" | ${SED} "s|\([^ ]*\)|${current_module}:\1|g" | tr ' ' ',')
                  chunks+=("\"$prefixed\"")
              fi
          done <<< "$(echo "$shuffled" | xargs -L ${TESTS_PER_CHUNK})"

          # Reset for new module
          module_tests=""
      fi

      current_module="$module"
      module_tests="${module_tests}${test} "
  done <<< "$test_list"

  # Process the last module
  if [ -n "$current_module" ] && [ -n "$module_tests" ]; then
      local shuffled=$(echo "$module_tests" | tr ' ' '\n' | grep -v '^$' | shuf)
      while IFS= read -r chunk; do
          if [ -n "$chunk" ]; then
              # Prefix each test with module: and convert spaces to commas
              local prefixed=$(echo "$chunk" | ${SED} "s|\([^ ]*\)|${current_module}:\1|g" | tr ' ' ',')
              chunks+=("\"$prefixed\"")
          fi
      done <<< "$(echo "$shuffled" | xargs -L ${TESTS_PER_CHUNK})"
  fi

  # Return chunks as array elements
  printf '%s\n' "${chunks[@]}"
}

# Discover IT tests (exclude KT patterns to avoid overlap)
IT_CHUNKS=$(discover_and_chunk "*IT.java" | grep -v "KT.java\"$" || true)

# Discover KT tests
KT_CHUNKS=$(discover_and_chunk "*KT.java")

# Combine chunks (IT first, then KT to preserve test ordering)
ALL_CHUNKS=()
while IFS= read -r chunk; do
  if [ -n "$chunk" ]; then
    ALL_CHUNKS+=("$chunk")
  fi
done <<< "$IT_CHUNKS"

while IFS= read -r chunk; do
  if [ -n "$chunk" ]; then
    ALL_CHUNKS+=("$chunk")
  fi
done <<< "$KT_CHUNKS"

# Output JSON array
TEST_CHUNKS="[$(IFS=,; echo "${ALL_CHUNKS[*]}")]"
echo "$TEST_CHUNKS"
