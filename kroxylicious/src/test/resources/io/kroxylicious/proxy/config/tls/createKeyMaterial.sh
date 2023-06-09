#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Generates key material to support DefaultSslContextFactory unit tests.
# Commit the resulting files to the repo.

set -o nounset -e

DNAME="cn=localhost"
KEYSIZE=2048
VALIDITY=3600
ALIAS=selfsigned
STOREPASS=storepass
KEYPASS=keypass

# Create JKS/PKCS12 keystore

rm -f server.jks server_diff_keypass.jks server.p12 client.jks client.p12
# store password and key password equal
keytool -genkey -keyalg RSA -alias ${ALIAS} -storetype JKS -keystore server.jks -storepass ${STOREPASS} -keypass ${STOREPASS} -validity ${VALIDITY} -keysize ${KEYSIZE} -dname ${DNAME}
# store password and key password different
keytool -genkey -keyalg RSA -alias ${ALIAS} -storetype JKS -keystore server_diff_keypass.jks -storepass ${STOREPASS} -keypass ${KEYPASS} -validity ${VALIDITY} -keysize ${KEYSIZE} -dname ${DNAME}

# Different store and key passwords not supported for PKCS12 KeyStores
keytool -genkey -keyalg RSA -alias ${ALIAS} -storetype PKCS12 -keystore server.p12 -storepass ${STOREPASS} -validity ${VALIDITY} -keysize ${KEYSIZE} -dname ${DNAME}

# Extract crt/key PEM from the PKCS12
openssl pkcs12 -in server.p12  -nokeys -out server.crt -passin pass:${STOREPASS}
openssl pkcs12 -in server.p12  -nodes -nocerts -out server.key -passin pass:${STOREPASS}
openssl pkcs8 -topk8 -passout pass:${KEYPASS} -in server.key -out server_encrypted.key

# Kafka supports PEM files containing both private key and certificate in the same file (KIP-651 ssl.keystore.key).
# The test data ensures that the artefacts can be expressed in any order.
cat server.crt server_encrypted.key > server_crt_encrypted_key.pem
cat server.key server.crt > server_key_crt.pem

# Create JKS/PKCS12 truststore

keytool -import -v -trustcacerts -alias ${ALIAS} -file server.crt -storetype JKS -keystore client.jks -storepass ${STOREPASS} -noprompt
keytool -import -v -trustcacerts -alias ${ALIAS} -file server.crt -storetype PKCS12 -keystore client.p12 -storepass ${STOREPASS} -noprompt

#
echo ${STOREPASS} > storepass.password
echo ${KEYPASS} > keypass.password
