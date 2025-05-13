#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Extracts fenced code blocks from the given markdown supplied on stdin, emitting them to stdout.
# If the fenced code block has an attribute `adjunct` its contents are emitted too, before the codeblock to which it is
# applied.
#
# Supports only codeblocks delimited by backticks.

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/common.sh"

BLOCKTYPE=${BLOCKTYPE:-yaml}
DOCS_DIR=${DOCS_DIR:-${SCRIPT_DIR}/../kroxylicious-docs}

#Find the files and then pass them to GAWK as args so it knows the file names
find "${DOCS_DIR}" -type f -name "*.adoc" -print0 | xargs -0 -n1 "${GAWK}" -f "${SCRIPT_DIR}/extract-codeblock.awk" -vBLOCKTYPE="${BLOCKTYPE}" "${0}"