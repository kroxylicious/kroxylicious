#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Register a client certificate with CipherTrust Manager for testing.
# This is a one-time admin operation to create a client that can authenticate
# using mutual TLS instead of username/password.
#
# Prerequisites:
#   - ksctl must be installed and logged in (ksctl login ...)
#   - jq must be installed
#
# Usage: register-client-cert.sh [output-directory]
#
# The script will:
#   1. Create a Local CA and self-sign it
#   2. Generate a CSR and Private Key for the Client
#   3. Issue and sign the client certificate
#   4. Register the client management profile
#   5. Create a token for client based on the profile
#   6. Register the client, linking the token to the client cert
#   7. Make web interface trust the new CA
#   8. Save the client certificate, key, CA ID, and client ID to the output directory
#
# Output files:
#   - kroxylicious-client.key - Client private key
#   - kroxylicious-client.crt - Client certificate
#   - kroxylicious-client-id.txt - Client ID (for KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_ID)
#   - kroxylicious-ca-id.txt - CA ID (for cleanup/reference)
#   - kroxylicious-profile-id.txt - Profile ID (for cleanup/reference)
#   - kroxylicious-token-id.txt - Registration token ID (for cleanup/reference)
#
# Example:
#   ./register-client-cert.sh /tmp
#   export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_ID=$(cat /tmp/kroxylicious-client-id.txt)
#   export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_CERT=/tmp/kroxylicious-client.crt
#   export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_KEY=/tmp/kroxylicious-client.key

set -euo pipefail

# Default output directory
OUTPUT_DIR="${1:-/tmp}"

# CA Common Name for the client certificate CA
# Note: ksctl ca locals create only supports --cn, which creates a subject like /CN=...
CA_CN="KroxyliciousClientCA"
CA_SUBJECT="/CN=${CA_CN}"

# Validate prerequisites
if ! command -v ksctl &> /dev/null; then
    echo "Error: ksctl is not installed or not in PATH" >&2
    echo "Download from your CipherTrust Manager: API -> CLI" >&2
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "Error: jq is not installed or not in PATH" >&2
    echo "Install with: brew install jq (macOS) or apt-get install jq (Linux)" >&2
    exit 1
fi

# Check if ksctl is logged in by attempting to list CAs
if ! ksctl ca locals list &> /dev/null; then
    echo "Error: Not logged in to CipherTrust Manager" >&2
    echo "Run: ksctl login --url <ctm-url> --user admin --nosslverify" >&2
    exit 1
fi

echo "==> Step 1: Creating Local CA and self-signing it..."
echo "    CA Subject: $CA_SUBJECT"
ksctl ca locals create --cn "$CA_CN" > /dev/null
CA_ID=$(ksctl ca locals list | jq -r --arg subject "$CA_SUBJECT" '.resources[] | select(.subject==$subject) | .id')
CA_URI=$(ksctl ca locals list | jq -r --arg subject "$CA_SUBJECT" '.resources[] | select(.subject==$subject) | .uri')
if [ -z "$CA_ID" ] || [ "$CA_ID" = "null" ]; then
    echo "Error: Failed to create Local CA" >&2
    exit 1
fi
echo "    Created CA ID: $CA_ID"
echo "    CA URI: $CA_URI"
echo "$CA_ID" > "$OUTPUT_DIR/kroxylicious-ca-id.txt"

ksctl ca locals self-sign --duration 3650 --id "${CA_ID}" > /dev/null
echo "    CA self-signed with 3650 day duration"

echo "==> Step 2: Generating CSR and Private Key for the Client..."
# Use RSA algorithm to generate PKCS#1 RSA keys which PemUtils converts
# to PKCS#8. The default EC algorithm generates PKCS#1 EC keys which
# PemUtils does not support.
ksctl ca csr \
  --cn "my-auth-client" \
  --alg RSA \
  --csr-outfile "$OUTPUT_DIR/kroxylicious-client.csr" \
  --key-outfile "$OUTPUT_DIR/kroxylicious-client.key" > /dev/null
echo "    Private key: $OUTPUT_DIR/kroxylicious-client.key"
echo "    CSR: $OUTPUT_DIR/kroxylicious-client.csr"

echo "==> Step 3: Issuing and signing the Client Certificate..."
ksctl ca locals certs issue \
  --ca-id "${CA_ID}" \
  --csr-infile "$OUTPUT_DIR/kroxylicious-client.csr" \
  --cert-outfile "$OUTPUT_DIR/kroxylicious-client.crt" \
  --duration 365 \
  --purpose client > /dev/null
echo "    Client certificate: $OUTPUT_DIR/kroxylicious-client.crt"

echo "==> Step 4: Registering the Client Management Profile..."
CLIENT_PROFILE_ID=$(ksctl clientmgmt profiles create --name 'kroxylicious-test' | jq -r .id)
if [ -z "$CLIENT_PROFILE_ID" ] || [ "$CLIENT_PROFILE_ID" = "null" ]; then
    echo "Error: Failed to create client management profile" >&2
    exit 1
fi
echo "    Created profile ID: $CLIENT_PROFILE_ID"
echo "$CLIENT_PROFILE_ID" > "$OUTPUT_DIR/kroxylicious-profile-id.txt"

# Note, for some reason, creating the profile with groups/ca_id doesn't seem to work, but updating does.
ksctl clientmgmt profiles update --profile-id "$CLIENT_PROFILE_ID" --ca_id "${CA_ID}" --groups 'admin' > /dev/null
echo "    Profile updated with CA ID and groups"

echo "==> Step 5: Creating token for client based on the profile..."
TOKEN_RESPONSE=$(ksctl clientmgmt tokens create --ca-id "${CA_ID}" --cert-duration 365 --client-mgmt-profile-id "${CLIENT_PROFILE_ID}")
REG_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r .token)
TOKEN_ID=$(echo "$TOKEN_RESPONSE" | jq -r .id)
if [ -z "$REG_TOKEN" ] || [ "$REG_TOKEN" = "null" ]; then
    echo "Error: Failed to create registration token" >&2
    exit 1
fi
echo "    Registration token created (ID: $TOKEN_ID)"
echo "$TOKEN_ID" > "$OUTPUT_DIR/kroxylicious-token-id.txt"

echo "==> Step 6: Registering the client, linking the token to the client cert..."
CLIENT_ID=$(ksctl clientmgmt clients register --reg-token "${REG_TOKEN}" --cert-file "$OUTPUT_DIR/kroxylicious-client.crt" | jq -r .id)
if [ -z "$CLIENT_ID" ] || [ "$CLIENT_ID" = "null" ]; then
    echo "Error: Failed to register client" >&2
    exit 1
fi
echo "    Client registered with ID: $CLIENT_ID"
echo "$CLIENT_ID" > "$OUTPUT_DIR/kroxylicious-client-id.txt"

echo "==> Step 7: Making web interface trust the new CA..."
CURRENT_TRUSTED_CAS=$(ksctl interfaces get --name web | jq -r '.trusted_cas.local | join(",")')
ksctl interfaces modify --name web --trusted-local-cas "${CURRENT_TRUSTED_CAS},${CA_URI}" > /dev/null
echo "    Web interface updated to trust new CA"

echo "==> Restarting web service to activate new certificate..."
ksctl services restart --service-names web -y > /dev/null
echo "    Web service restarted"

# Clean up temporary files
rm -f "$OUTPUT_DIR/kroxylicious-client.csr"

echo ""
echo "==> Client registration complete!"
echo ""
echo "Client ID: $CLIENT_ID"
echo "CA ID: $CA_ID"
echo "Profile ID: $CLIENT_PROFILE_ID"
echo "Token ID: $TOKEN_ID"
echo ""
echo "Files created in $OUTPUT_DIR:"
echo "  - kroxylicious-client.key (private key)"
echo "  - kroxylicious-client.crt (certificate)"
echo "  - kroxylicious-client-id.txt (client ID)"
echo "  - kroxylicious-ca-id.txt (CA ID)"
echo "  - kroxylicious-profile-id.txt (profile ID)"
echo "  - kroxylicious-token-id.txt (token ID)"
echo ""
echo "To use with integration tests, set these environment variables:"
echo ""
echo "  export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_ID=$CLIENT_ID"
echo "  export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_CERT=$OUTPUT_DIR/kroxylicious-client.crt"
echo "  export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_KEY=$OUTPUT_DIR/kroxylicious-client.key"
echo ""
