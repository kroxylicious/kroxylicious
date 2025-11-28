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
  echo "${full_dir}"
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
  local lib_path="${1}"
  arch="${TARGETARCH:-$(uname -m)}"
  native_lib="${NATIVE_LIB_BASE_DIR}${lib_path}/${TARGETOS:-linux}"
  if [ -r "${native_lib}" ]; then
    if [ -r "${native_lib}/${arch}" ]; then
      echo "${native_lib}/${arch}"
      return
    else
      # not everyone thinks `uname -m` is the right convention
      case ${arch} in
        arm64) arch=aarch64 ;;
        x86_64) arch=amd64 ;;
      esac
      if [ -r "${native_lib}/${arch}" ]; then
          echo "${native_lib}/${arch}"
          return
      fi
    fi
  elif [ -r "${NATIVE_LIB_BASE_DIR}${lib_path}/" ]; then
    # The native dependency isn't broken down by OS and arch but still has something loadable. (Looking at you Netty)
    echo "${NATIVE_LIB_BASE_DIR}${lib_path}/"
    return
  fi
}

if [ "${KROXYLICIOUS_LOGGING_OPTIONS+set}" != set ]; then
  KROXYLICIOUS_LOGGING_OPTIONS="-Dlog4j2.configurationFile=$(script_dir)/../config/log4j2.yaml -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
fi

NATIVE_LIB_BASE_DIR=${NATIVE_LIB_BASE_DIR:-$(script_dir)/../libs/native/}
NETTY_NATIVE_LIB=$(native_library_path netty)
LZ4_NATIVE_LIB=$(native_library_path lz4-java/net/jpountz/util)
SNAPPY_NATIVE_LIB=$(native_library_path snappy/org/xerial/snappy/native)
ZSTD_NATIVE_LIB=$(native_library_path zstd-jni)
ZSTD_FULLY_QUALIFIED=$(ls "${ZSTD_NATIVE_LIB}"/libzstd-jni-*)
ZSTD_FULLY_QUALIFIED=$(ls "${ZSTD_NATIVE_LIB}"/libzstd-jni-*.so)

ASYNC_PROFILER_FULLY_QUALIFIED="$(native_library_path async-profiler)/libasyncProfiler.so"
echo "Async profiler available at: ${ASYNC_PROFILER_FULLY_QUALIFIED} use the ASYNC_PROFILER_FLAGS to control its settings"

if [ -n "${ASYNC_PROFILER_ENABLED:-}" ] || [ -n "${ASYNC_PROFILER_FLAGS:-}" ]; then
  echo "profiler flags${ASYNC_PROFILER_FLAGS:+=${ASYNC_PROFILER_FLAGS}}"
  JAVA_OPTIONS="-agentpath:${ASYNC_PROFILER_FULLY_QUALIFIED}${ASYNC_PROFILER_FLAGS:+=${ASYNC_PROFILER_FLAGS}} ${JAVA_OPTIONS}"
fi

NATIVE_LIB_PATH="${NATIVE_LIB_PATH:-${NETTY_NATIVE_LIB}:${LZ4_NATIVE_LIB}:${SNAPPY_NATIVE_LIB}:${ZSTD_NATIVE_LIB}:${LD_LIBRARY_PATH:-}}"

NATIVE_LIB_OPTIONS=${NATIVE_LIB_OPTIONS:-"-Djava.library.path=${NATIVE_LIB_PATH} -Dorg.xerial.snappy.disable.bundled.libs=true -DZstdNativePath=${ZSTD_FULLY_QUALIFIED}"}

export JAVA_OPTIONS="${KROXYLICIOUS_LOGGING_OPTIONS:-} ${NATIVE_LIB_OPTIONS:-} ${JAVA_OPTIONS:-}"
JAVA_CLASSPATH="$(classpath)"
export JAVA_CLASSPATH
export JAVA_MAIN_CLASS=io.kroxylicious.app.Kroxylicious
exec "$(script_dir)"/run-java.sh "$@"

