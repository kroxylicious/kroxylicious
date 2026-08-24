#!/bin/sh
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Disable warnings about `local` variables
# shellcheck disable=SC3043
script_dir() {
  local dir
  dir=$(dirname "$0")
  local full_dir
  full_dir=$(cd "${dir}" && pwd)
  echo "${full_dir}"
}

if [ "${CREDENTIAL_TOOL_LOGGING_OPTIONS+set}" != set ]; then
  CREDENTIAL_TOOL_LOGGING_OPTIONS="-Dlog4j2.configurationFile=$(script_dir)/../config/log4j2-tools.yaml"
fi

export JAVA_OPTIONS="${CREDENTIAL_TOOL_LOGGING_OPTIONS:-} ${JAVA_OPTIONS:-}"
export DEBUG_OUTPUT=/dev/null
export HIDE_CMD_LINE=1
JAVA_CLASSPATH="$(script_dir)/../libs/*"
export JAVA_CLASSPATH
export JAVA_MAIN_CLASS=io.kroxylicious.scram.credentialstore.file.cli.ScramCredentialFileTool
exec "$(script_dir)"/run-java.sh "$@"
