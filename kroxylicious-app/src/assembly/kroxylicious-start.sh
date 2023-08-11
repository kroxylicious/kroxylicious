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

export JAVA_CLASSPATH="$(script_dir)/../libs/*"
export JAVA_MAIN_CLASS=io.kroxylicious.app.Kroxylicious
exec $(script_dir)/run-java.sh "$@"

