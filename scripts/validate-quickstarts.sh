#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Validates Asciidoc YAML code blocks are valid yaml, failing otherwise

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/common.sh"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NOCOLOR='\033[0m'
BLOCKTYPE=${BLOCKTYPE:-console}
DOCS_DIR=${DOCS_DIR:-${SCRIPT_DIR}/../docs}

validate() {
    QUICKSTART=$1
    VARIANT=$2
    VARS=("-v" "CHECK_CMD=cat" "-v" "BLOCKTYPE=${BLOCKTYPE}")
    if [[ ${VARIANT} ]]; then
      VARS+=("-v" "INC_LABELLED_CBS=variant=${VARIANT}")
    fi
    # Extract the code blocks from each quickstart
    ${GAWK} "${VARS[@]}" -f ${SCRIPT_DIR}/extract-asciidoc-codeblock.awk ${QUICKSTART} | \
       ${SED} -e 's/^\$ //g' | \
       (
          # We eval the commands from the quickstart in a sub-shell so that side effects are discarded after each validation.
          while read -r CMD
          do
              echo -e  "${YELLOW}Executing '${CMD}'${NOCOLOR}"

              eval ${CMD}
              echo -e "${YELLOW}Finished executing '${CMD}${NOCOLOR}"
          done
       )
}

err() {
    echo -e "${RED}Failed validating ${QUICKSTART}${NOCOLOR}"
}

trap err ERR

# Find the quickstarts

for QUICKSTART in $(find ${DOCS_DIR} -path "*quickstart/index.adoc")
do

  VARIANTS=$($GAWK '/^\/\/ *variants:/ {split($3, variants, "|"); for (v in variants) print variants[v]}' ${QUICKSTART})
  if [[ ${VARIANTS} ]]; then
      for VARIANT in ${VARIANTS}
      do
          echo -e "${GREEN}Validating ${QUICKSTART} variant ${VARIANT}${NOCOLOR}"
          validate ${QUICKSTART} ${VARIANT}
      done
  else
      echo -e "${GREEN}Validating ${QUICKSTART}${NOCOLOR}"
      validate ${QUICKSTART} ""
  fi
done

echo -e "${GREEN}All done${NOCOLOR}"
