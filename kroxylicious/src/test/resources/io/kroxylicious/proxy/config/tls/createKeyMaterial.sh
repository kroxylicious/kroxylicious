#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Generates key material to support the io.kroxylicious.proxy.config.tls unit tests.
# Commit the resulting files to the repo.

set -o nounset -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
OPENSSL=openssl
KEYTOOL=keytool
GIT=git

command -v ${OPENSSL} >/dev/null 2>&1 || { echo >&2 "${OPENSSL} is not found on the path"; exit 1; }
command -v ${KEYTOOL} >/dev/null 2>&1 || { echo >&2 "${KEYTOOL} is not found on the path"; exit 1; }
command -v ${GIT} >/dev/null 2>&1 || { echo >&2 "${GIT} is not found on the path"; exit 1; }

DNAME="cn=localhost"
KEYSIZE=2048
VALIDITY=3600
ALIAS=selfsigned
STOREPASS=storepass
KEYPASS=keypass

cd ${DIR}

find ${DIR} \( -type f -maxdepth 1 -name "client*" -o -name "server*" -o -name "*pass.password" \) -delete

# Create JKS/PKCS12 keystore

# store password and key password equal
${KEYTOOL} -genkey -keyalg RSA -alias ${ALIAS} -storetype JKS -keystore server.jks -storepass ${STOREPASS} -keypass ${STOREPASS} -validity ${VALIDITY} -keysize ${KEYSIZE} -dname ${DNAME}
# store password and key password different
${KEYTOOL}  -genkey -keyalg RSA -alias ${ALIAS} -storetype JKS -keystore server_diff_keypass.jks -storepass ${STOREPASS} -keypass ${KEYPASS} -validity ${VALIDITY} -keysize ${KEYSIZE} -dname ${DNAME}

# Note: Different store and key passwords not supported for PKCS12 KeyStores
${KEYTOOL}  -genkey -keyalg RSA -alias ${ALIAS} -storetype PKCS12 -keystore server.p12 -storepass ${STOREPASS} -validity ${VALIDITY} -keysize ${KEYSIZE} -dname ${DNAME}

# Extract crt (X.509) / key (PKCS8) as PEM from the PKCS12
${OPENSSL} pkcs12 -in server.p12  -nokeys -out server.crt -passin pass:${STOREPASS}
${OPENSSL} pkcs12 -in server.p12  -nodes -nocerts -out server.key -passin pass:${STOREPASS}
${OPENSSL} pkcs8 -topk8 -passout pass:${KEYPASS} -in server.key -out server_encrypted.key

# Kafka supports PEM files containing both private key and certificate in the same file (KIP-651 ssl.keystore.key).
# The test data ensures that the artefacts can be expressed in any order.
cat server.crt server_encrypted.key > server_crt_encrypted_key.pem
cat server.key server.crt > server_key_crt.pem

# Create JKS/PKCS12 truststore
${KEYTOOL} -import -v -trustcacerts -alias ${ALIAS} -file server.crt -storetype JKS -keystore client.jks -storepass ${STOREPASS} -noprompt
${KEYTOOL} -import -v -trustcacerts -alias ${ALIAS} -file server.crt -storetype PKCS12 -keystore client.p12 -storepass ${STOREPASS} -noprompt

# Create files containing the passwords.
echo ${STOREPASS} > storepass.password
echo ${KEYPASS} > keypass.password

${GIT} add .
