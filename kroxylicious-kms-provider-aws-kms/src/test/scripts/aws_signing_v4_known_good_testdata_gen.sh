#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Generates known good test data for the AWS Request Signing Tests
# Usage: aws_signing_v4_known_good_testdata_gen.sh > test/resources/

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. "${SCRIPT_DIR}/../../../../scripts/common.sh"
ECHOING_HTTP_SERVER=${SCRIPT_DIR}/echoing_http_server.py
CURL=$(resolveCommand curl)
INPUT_YAML=${1?Usage $0 <input.yaml>}

function awaitPortOpen() {
  local PORT
  PORT=$1
  while ! nc -z localhost ${PORT} 1>/dev/null 2>/dev/null; do
    sleep 0.1
  done
}

function startServer() {
  local PORT
  PORT=$1
  $ECHOING_HTTP_SERVER ${PORT} &
  SERVER_PID=$!
  trap "kill ${SERVER_PID} || true" EXIT
  awaitPortOpen ${PORT}
  echo Server started on port ${PORT} >/dev/stderr
}

function invoke_curl() {
  ${CURL} --silent "${@}"
}

cat << EOF
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Known good test data for AWS v4 request signing

EOF

echo Starting server 1>/dev/stderr
startServer 4566

FIRST=true
IFS='|'
yq eval-all '. | [[
. | @json,
.url,
"--user",
.accessKeyId + ":" +  .secretAccessKey,
"--aws-sigv4",
 "aws:amz:" + .region + ":" + .service,
"--request",
.method,
((.data | "--data-raw" + "|" + .) // null),
((.headers[] | ((.[] | "--header" + "|" + ((parent | key) + ": " + .)))
               // ("--header" + "|" + key + ":")) // null)
]
| del( .[] | select(. == null))
| join("|") ]
| join("\n")
' ${INPUT_YAML} | while  read -ra ALL; do
  TEST_DEF_JSON=${ALL[0]}
  CURL_ARGS=(${ALL[@]:1})
  if [[ ${FIRST} == "false" ]]; then
    echo ---
  fi
  invoke_curl ${CURL_ARGS[@]} | jq --argjson testDef "${TEST_DEF_JSON}" \
                                       '$testDef * . | del(..|select(. == ""))' | yq -P
  FIRST=false
done


invoke_curl http://localhost:4566/bye 1>/dev/null

echo "All done" >/dev/stderr
