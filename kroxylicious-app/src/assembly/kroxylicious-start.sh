#!/bin/sh
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

script_dir() {
  # Default is current directory
  local dir=$(dirname "$0")
  local full_dir=$(cd "${dir}" && pwd)
  echo ${full_dir}
}

classpath() {
  local class_path="$(script_dir)/../libs/*"
  if [ -n "${KROXYLICIOUS_CLASSPATH:-}" ]; then
    class_path="$class_path:${KROXYLICIOUS_CLASSPATH}"
  fi
  echo "${class_path}"
}

export JAVA_OPTIONS="${JAVA_OPTIONS:-} -Dlog4j2.configurationFile=$(script_dir)/../config/log4j2.yml"
export JAVA_CLASSPATH="$(classpath)"
export JAVA_MAIN_CLASS=io.kroxylicious.app.Kroxylicious
exec $(script_dir)/run-java.sh "$@"

