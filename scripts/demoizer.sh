#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Executes commands from a file, one by one, typing each command to the terminal (at human speed) to
# give the impression of an interactive session.
#
# Intended to be used with asciinema in a command like this:
#
# extract-markdown-fencedcodeblocks.sh < markdown.md > cmds.txt
# asciinema rec --overwrite --command './scripts/demoizer.sh cmds.txt .' out.cast

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <COMMAND_FILE> <WORKING_DIR>"
  exit 1
fi
COMMAND_FILE=$1
WORKING_DIR=$2
if [[ ! -f "${COMMAND_FILE}"  ]]; then
  echo "$0: Command file ${COMMAND_FILE} does not exist."
  exit 1
fi

if [[ ! -d "${WORKING_DIR}"  ]]; then
  echo "$0: Working directory ${WORKING_DIR} does not exist."
  exit 1
fi

exec 3< ${COMMAND_FILE}
cd ${WORKING_DIR}
# expects a single command per line, no continuation supported
while read -u 3 -r CMD
do
    # give the appearance of a terminal command prompt.
    echo -n '$ '
    # echo the command to the tty using expects send human feature (to emulate typing)
    expect -f - "${CMD}" <<- 'EOF'
    set cmd [lindex $argv 0];

    set send_human {.03 .1 1 .05 0.5}
    send_tty -h "${cmd}\n"
EOF

    # demo commands may legitimately fail, so carry on regardless.
    eval ${CMD} || true
done


