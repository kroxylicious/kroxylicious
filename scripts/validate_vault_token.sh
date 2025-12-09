#!/usr/bin/env sh
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Tests that a Vault Token has sufficient privileges to perform the operations required for Envelope Encryption.
# Usage: VAULT_TOKEN=<kroxylicious encryption filter token> validate_vault_token.sh <kek path>

set -euo pipefail

KEK_PATH=${1:-transit/keys/KEK-testkey}

echo -n "Testing ability to read key"

NAME=$(vault read -field=name ${KEK_PATH})
echo " - Ok"

echo -n "Testing ability to create datakey"

CIPHERTEXT=$(vault write -field=ciphertext -force transit/datakey/plaintext/${NAME})
echo " - Ok"

echo -n "Testing ability to decrypt encrypted datakey"
vault write transit/decrypt/${NAME} ciphertext=${CIPHERTEXT} 1>/dev/null
echo " - Ok"