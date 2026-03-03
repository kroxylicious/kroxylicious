#! /bin/bash
set -euo pipefail

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" # cd to the dir containing this script
KROXYLICIOUS_VERSION=${KROXYLICIOUS_VERSION:-@project.version@}
KROXYLICIOUS_CACHE="${HOME}/.kroxylicious"
KROXYLICIOUS_HOME="${KROXYLICIOUS_HOME:-${KROXYLICIOUS_CACHE}/kroxylicious-app-${KROXYLICIOUS_VERSION}}"
if [ ! -d "${KROXYLICIOUS_HOME}" ]; then
  echo "Kroxylicious not found. Downloading and installing..."
  mkdir -p ${KROXYLICIOUS_CACHE}
  DISTRIBUTION="https://github.com/kroxylicious/kroxylicious/releases/download/v${KROXYLICIOUS_VERSION}/kroxylicious-app-${KROXYLICIOUS_VERSION}-bin.tar.gz"
  curl -fsSL "${DISTRIBUTION}" | tar xzf - -C "${KROXYLICIOUS_CACHE}/"
  echo "Installed kroxylicious into ${KROXYLICIOUS_HOME}"
fi
if command -v podman &> /dev/null; then
    echo "✅ Podman is installed."
    CONTAINER_ENGINE="podman"
elif command -v docker &> /dev/null; then
    echo "✅ Docker is installed."
    CONTAINER_ENGINE="docker"
else
    echo "❌ Neither podman or docker installed. Please install it to continue."
    exit 1
fi
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven is not installed. Please install it to continue."
fi
CONTAINER_ID=$(${CONTAINER_ENGINE} run --rm -p 9092:9092 --detach apache/kafka:@kafka.version@)
if [ -z "$CONTAINER_ID" ]; then
  echo "Failed to start kafka."
  exit 1
fi
echo "Kafka started with ID: $CONTAINER_ID"
cleanup() {
  echo "Script exiting. Cleaning up container $CONTAINER_ID..."
  ${CONTAINER_ENGINE} kill "$CONTAINER_ID"
  echo "Container killed."
}
trap cleanup EXIT

echo "Building project"
mvn clean package -DskipTests
export KROXYLICIOUS_CLASSPATH="target/*"
${KROXYLICIOUS_HOME}/bin/kroxylicious-start.sh "$@"
