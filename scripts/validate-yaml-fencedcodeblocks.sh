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

BLOCKTYPE=${BLOCKTYPE:-yaml}
DOCS_DIR=${DOCS_DIR:-${SCRIPT_DIR}/../docs}

#Find the files and then pass them to GAWK as args so it knows the file names
find "${DOCS_DIR}" -type f -name "*.adoc" -print0 | xargs -0 -n1 "${GAWK}" -f "${SCRIPT_DIR}/extract-asciidoc-codeblock.awk" -vCHECK_CMD="yq 'true' > /dev/null" -vBLOCKTYPE="${BLOCKTYPE}" "${0}"
