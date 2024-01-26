#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# generate private key and self-signed-certificate for vault
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 99999 -subj "/CN=localhost" -nodes
# generate truststore for java client (entered password changeit manually)
keytool -importcert -storetype PKCS12 -keystore vault-truststore.pkcs12 -alias vault -file cert.pem -noprompt
