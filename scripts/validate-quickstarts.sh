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

#Find the files and then pass them to GAWK as args so it knows the file names

err() {
    echo -e  "${RED}Failed validating ${QUICKSTART}}${NOCOLOR}"
}

trap err ERR


for QUICKSTART in $(find ${DOCS_DIR} -path "*quickstart/index.adoc")
do
  echo -e  "${GREEN}Validating ${QUICKSTART}}${NOCOLOR}"

  ${GAWK} -v CHECK_CMD=cat -v BLOCKTYPE=${BLOCKTYPE} -v EXCLUDED_ATTRS="variant=localstack" -f ${SCRIPT_DIR}/extract-asciidoc-codeblock.awk ${QUICKSTART} | \
     ${SED} -e 's/^\$ //g' | \
     tee /tmp/kw |
     (
        while read -r CMD
        do
            echo -e  "${YELLOW}Executing '${CMD}${NOCOLOR}"

            eval ${CMD}
            echo -e  "${YELLOW}Finished executing '${CMD}${NOCOLOR}"
        done
     )
done

echo -e "${GREEN}All done${NOCOLOR}"
