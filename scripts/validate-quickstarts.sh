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

BLOCKTYPE=${BLOCKTYPE:-console}
DOCS_DIR=${DOCS_DIR:-${SCRIPT_DIR}/../docs}

#Find the files and then pass them to GAWK as args so it knows the file names

cleanup() {
    echo "Cleanup"
}

trap cleanup EXIT


for QUICKSTART in $(find ${DOCS_DIR} -path "*quickstart/index.adoc")
do
  echo Testing ${QUICKSTART}
  ${GAWK} -v CHECK_CMD=cat -v BLOCKTYPE=${BLOCKTYPE} -v EXCLUDED_ATTRS="variant=localstack" -f ${SCRIPT_DIR}/extract-asciidoc-codeblock.awk ${QUICKSTART} | \
     ${SED} -e 's/^\$ //g' | \
     tee /tmp/kw |
     (
#        while read -r CMD
#        do
#            echo "Executing '${CMD}'"
#        done
        while read -r CMD
        do
            echo "Executing '${CMD}'"
            eval ${CMD}
            echo "Finished executing, return code $?"
        done
        echo "Done cmd loop"
     )
done
echo "Done quickstart loop"
