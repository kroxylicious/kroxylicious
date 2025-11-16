#!/bin/sh
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Disable warnings about `local` variables
# shellcheck disable=SC3043
script_dir() {
  # Default is current directory
  local dir
  dir=$(dirname "$0")
  local full_dir
  full_dir=$(cd "${dir}" && pwd)
  echo ${full_dir}
}

# Disable warnings about `local` variables
# shellcheck disable=SC3043
classpath() {
  local class_path
  class_path="$(script_dir)/../libs/*"
  if [ -n "${KROXYLICIOUS_CLASSPATH:-}" ]; then
    class_path="$class_path:${KROXYLICIOUS_CLASSPATH}"
  fi
  echo "${class_path}"
}

# Disable warnings about `local` variables
# shellcheck disable=SC3043
native_library_path() {
  NATIVE_LIB_BASE_DIR="/opt/kroxylicious/libs/native/"
  local lib_path="${1}"
  local arch="${TARGETARCH}"
  case ${TARGETARCH} in
    arm64) arch=aarch64
  esac
  native_lib="${NATIVE_LIB_BASE_DIR}${lib_path}/${TARGETOS}/${arch}"
  if [ -r "${native_lib}" ]; then
    echo "${native_lib}"
  fi
}

if [ "${KROXYLICIOUS_LOGGING_OPTIONS+set}" != set ]; then
  KROXYLICIOUS_LOGGING_OPTIONS="-Dlog4j2.configurationFile=$(script_dir)/../config/log4j2.yaml -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
fi

LZ4_NATIVE_LIB=$(native_library_path lz4-java/net/jpountz/util)
SNAPPY_NATIVE_LIB=$(native_library_path snappy/org/xerial/snappy/native)
ZSTD_NATIVE_LIB=$(native_library_path zstd-jni)
NATIVE_LIB_PATH="${LZ4_NATIVE_LIB}:${SNAPPY_NATIVE_LIB}:${ZSTD_NATIVE_LIB}"

echo "setting java.library.path=${NATIVE_LIB_PATH}"
NATIVE_LIB_OPTIONS="-Djava.library.path=${NATIVE_LIB_PATH} -Dorg.xerial.snappy.disable.bundled.libs=true"

export JAVA_OPTIONS="${KROXYLICIOUS_LOGGING_OPTIONS} ${NATIVE_LIB_OPTIONS} ${JAVA_OPTIONS:-}"
JAVA_CLASSPATH="$(classpath)"
export JAVA_CLASSPATH
export JAVA_MAIN_CLASS=io.kroxylicious.app.Kroxylicious
exec "$(script_dir)"/run-java.sh "$@"

