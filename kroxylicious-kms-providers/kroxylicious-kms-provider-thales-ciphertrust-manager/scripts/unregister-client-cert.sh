#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Unregister a client certificate from CipherTrust Manager.
# This removes the client, profile, and CA from CTM and optionally deletes local credential files.
#
# Prerequisites:
#   - ksctl must be installed and logged in (ksctl login ...)
#   - jq must be installed
#
# Usage: unregister-client-cert.sh [output-directory]
#
# Arguments:
#   [output-directory]  - Directory containing kroxylicious-client-id.txt, kroxylicious-ca-id.txt,
#                         and kroxylicious-profile-id.txt (default: /tmp)
#
# The script will:
#   1. Delete the client from CipherTrust Manager
#   2. Delete the client management profile
#   3. Delete the local CA
#   4. Optionally delete local credential files
#
# Examples:
#   ./unregister-client-cert.sh /tmp

set -euo pipefail

# Default output directory
OUTPUT_DIR="${1:-/tmp}"

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

# Check if ksctl is logged in
if ! ksctl ca locals list &> /dev/null; then
    echo "Error: Not logged in to CipherTrust Manager" >&2
    echo "Run: ksctl login --url <ctm-url> --user admin --nosslverify" >&2
    exit 1
fi

# Read IDs from files
if [ ! -f "$OUTPUT_DIR/kroxylicious-client-id.txt" ]; then
    echo "Error: Client ID file not found: $OUTPUT_DIR/kroxylicious-client-id.txt" >&2
    exit 1
fi
CLIENT_ID=$(cat "$OUTPUT_DIR/kroxylicious-client-id.txt")
echo "Read client ID from file: $CLIENT_ID"

if [ ! -f "$OUTPUT_DIR/kroxylicious-profile-id.txt" ]; then
    echo "Warning: Profile ID file not found: $OUTPUT_DIR/kroxylicious-profile-id.txt" >&2
    echo "         Profile cleanup will be skipped" >&2
    PROFILE_ID=""
else
    PROFILE_ID=$(cat "$OUTPUT_DIR/kroxylicious-profile-id.txt")
    echo "Read profile ID from file: $PROFILE_ID"
fi

if [ ! -f "$OUTPUT_DIR/kroxylicious-ca-id.txt" ]; then
    echo "Warning: CA ID file not found: $OUTPUT_DIR/kroxylicious-ca-id.txt" >&2
    echo "         CA cleanup will be skipped" >&2
    CA_ID=""
else
    CA_ID=$(cat "$OUTPUT_DIR/kroxylicious-ca-id.txt")
    echo "Read CA ID from file: $CA_ID"
fi

if [ ! -f "$OUTPUT_DIR/kroxylicious-token-id.txt" ]; then
    echo "Warning: Token ID file not found: $OUTPUT_DIR/kroxylicious-token-id.txt" >&2
    echo "         Token cleanup will be skipped" >&2
    TOKEN_ID=""
else
    TOKEN_ID=$(cat "$OUTPUT_DIR/kroxylicious-token-id.txt")
    echo "Read token ID from file: $TOKEN_ID"
fi

echo ""
echo "==> Step 1: Deleting client from CipherTrust Manager..."
if ksctl clientmgmt clients delete --client-id "$CLIENT_ID" --nowarning &> /dev/null; then
    echo "    Client deleted (ID: $CLIENT_ID)"
else
    echo "Warning: Failed to delete client from CipherTrust Manager" >&2
    echo "         Client may not exist or you may not have permission" >&2
fi

if [ -n "$TOKEN_ID" ]; then
    echo "==> Step 2: Deleting registration token..."
    if ksctl clientmgmt tokens delete --token-id "$TOKEN_ID" &> /dev/null; then
        echo "    Token deleted (ID: $TOKEN_ID)"
    else
        echo "Warning: Failed to delete token from CipherTrust Manager" >&2
        echo "         Token may not exist or you may not have permission" >&2
    fi
else
    echo "==> Step 2: Skipping token deletion (no token ID found)"
fi

if [ -n "$PROFILE_ID" ]; then
    echo "==> Step 3: Deleting client management profile..."
    if ksctl clientmgmt profiles delete --profile-id "$PROFILE_ID" &> /dev/null; then
        echo "    Profile deleted (ID: $PROFILE_ID)"
    else
        echo "Warning: Failed to delete profile from CipherTrust Manager" >&2
        echo "         Profile may not exist or you may not have permission" >&2
    fi
else
    echo "==> Step 3: Skipping profile deletion (no profile ID found)"
fi

if [ -n "$CA_ID" ]; then
    echo "==> Step 4: Removing CA from web interface trusted CAs..."
    # Get current trusted CAs
    CA_URI=$(ksctl ca locals list | jq -r --arg id "$CA_ID" '.resources[] | select(.id==$id) | .uri')
    if [ -n "$CA_URI" ] && [ "$CA_URI" != "null" ]; then
        CURRENT_TRUSTED_CAS=$(ksctl interfaces get --name web | jq -r '.trusted_cas.local | join(",")')
        # Remove our CA URI from the list
        NEW_TRUSTED_CAS=$(echo "$CURRENT_TRUSTED_CAS" | tr ',' '\n' | grep -v "$CA_URI" | tr '\n' ',' | sed 's/,$//')

        if [ "$CURRENT_TRUSTED_CAS" != "$NEW_TRUSTED_CAS" ]; then
            ksctl interfaces modify --name web --trusted-local-cas "$NEW_TRUSTED_CAS" > /dev/null
            echo "    CA removed from web interface"

            echo "==> Restarting web service..."
            ksctl services restart --service-names web -y > /dev/null
            echo "    Web service restarted"
        else
            echo "    CA was not in web interface trusted CAs list"
        fi
    else
        echo "    CA URI not found, skipping web interface update"
    fi

    echo "==> Step 5: Deleting local CA..."
    if ksctl ca locals delete --id "$CA_ID" &> /dev/null; then
        echo "    CA deleted (ID: $CA_ID)"
    else
        echo "Warning: Failed to delete CA from CipherTrust Manager" >&2
        echo "         CA may not exist or you may not have permission" >&2
    fi
else
    echo "==> Step 4: Skipping CA removal from web interface (no CA ID found)"
    echo "==> Step 5: Skipping CA deletion (no CA ID found)"
fi

# Check for local files and offer to delete them
FILES_TO_DELETE=(
    "$OUTPUT_DIR/kroxylicious-client.key"
    "$OUTPUT_DIR/kroxylicious-client.crt"
    "$OUTPUT_DIR/kroxylicious-client-id.txt"
    "$OUTPUT_DIR/kroxylicious-ca-id.txt"
    "$OUTPUT_DIR/kroxylicious-profile-id.txt"
    "$OUTPUT_DIR/kroxylicious-token-id.txt"
)

FILES_FOUND=()
for file in "${FILES_TO_DELETE[@]}"; do
    if [ -f "$file" ]; then
        FILES_FOUND+=("$file")
    fi
done

if [ ${#FILES_FOUND[@]} -gt 0 ]; then
    echo ""
    echo "==> Deleting local credential files from $OUTPUT_DIR:"
    for file in "${FILES_FOUND[@]}"; do
        rm -f "$file"
        echo "    Deleted: $(basename "$file")"
    done
    echo ""
    echo "==> Local files deleted"
else
    echo ""
    echo "==> No local credential files found in $OUTPUT_DIR"
fi

echo ""
echo "==> Client unregistration complete!"
echo ""
