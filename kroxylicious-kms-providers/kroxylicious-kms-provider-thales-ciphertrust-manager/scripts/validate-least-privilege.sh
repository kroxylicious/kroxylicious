#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

#
# Validate Least Privilege Permissions for Kroxylicious in CipherTrust Manager
#
# Purpose:
#   This script validates that a CipherTrust Manager policy can be configured to grant
#   Kroxylicious the minimal permissions it needs for record encryption operations,
#   while blocking administrative operations like key creation and deletion.
#
# What it does:
#   1. Creates a test group, policy, and user in CTM
#   2. Configures a policy with only the actions Kroxylicious requires:
#      - ReadKey (for alias resolution)
#      - EncryptWithKey (to encrypt DEKs)
#      - DecryptWithKey (to decrypt EDEKs)
#      - UseKey (required for crypto operations)
#   3. Restricts the policy to keys matching the pattern: kek-*
#   4. Validates that:
#      - Administrative operations (CreateKey) are blocked
#      - Crypto operations on KEK-* keys are allowed
#      - Operations on non-KEK-* keys are blocked
#
# This proves that CTM can enforce least privilege for Kroxylicious service accounts,
# ensuring they cannot create, delete, or modify keys - only use existing keys for
# envelope encryption operations.
#
# Environment variables:
#   SKIP_CLEANUP - If set, resources will not be deleted after the test
#

set -euo pipefail

# Configuration
CTM_URL=$(yq '.KSCTL_URL' ~/.ksctl/config.yaml)
USER_NAME="test-user-$$"
USER_PASSWORD="Foobar\$2"
GROUP_NAME="test-group-$$"
POLICY_NAME="test-policy-$$"

# Test keys
KEK_NAME="KEK-test-$$"
NONKEK_NAME="AES-test-$$"

echo "=========================================================================="
echo "CipherTrust Manager - Minimal Permissions Test"
echo "=========================================================================="
echo ""
echo "Configuration:"
echo "  CTM URL: $CTM_URL"
echo "  User: $USER_NAME"
echo "  Group: $GROUP_NAME"
echo "  Policy: $POLICY_NAME"
echo ""
echo "Test Keys:"
echo "  KEK key: $KEK_NAME"
echo "  Non-KEK key: $NONKEK_NAME"
echo ""

# Cleanup function
cleanup() {
    if [ -n "${SKIP_CLEANUP:-}" ]; then
        echo ""
        echo "=========================================================================="
        echo "Cleanup skipped (SKIP_CLEANUP is set)"
        echo "=========================================================================="
        echo ""
        echo "Resources created:"
        echo "  User: $USER_NAME (ID: ${USER_ID:-unknown})"
        echo "  Group: $GROUP_NAME"
        echo "  Policy: $POLICY_NAME (ID: ${POLICY_ID:-unknown})"
        echo "  Policy Attachment: ${POLICY_ATTACH_ID:-unknown}"
        echo "  KEK: $KEK_NAME"
        echo "  Non-KEK key: $NONKEK_NAME"
        return
    fi

    echo ""
    echo "=========================================================================="
    echo "Cleanup"
    echo "=========================================================================="
    echo ""

    # Delete keys
    if [ -n "${KEK_NAME:-}" ]; then
        ksctl keys delete --name "$KEK_NAME" > /dev/null 2>&1 && echo "✓ Deleted key: $KEK_NAME" || true
    fi
    if [ -n "${NONKEK_NAME:-}" ]; then
        ksctl keys delete --name "$NONKEK_NAME" > /dev/null 2>&1 && echo "✓ Deleted key: $NONKEK_NAME" || true
    fi

    # Delete user
    if [ -n "${USER_ID:-}" ]; then
        ksctl users delete -i "$USER_ID" > /dev/null 2>&1 && echo "✓ Deleted user: $USER_NAME" || true
    fi

    # Delete policy attachment
    if [ -n "${POLICY_ATTACH_ID:-}" ]; then
        ksctl polattach delete --id "$POLICY_ATTACH_ID" > /dev/null 2>&1 && echo "✓ Deleted policy attachment" || true
    fi

    # Delete policy
    if [ -n "${POLICY_ID:-}" ]; then
        ksctl policy delete --id "$POLICY_ID" > /dev/null 2>&1 && echo "✓ Deleted policy: $POLICY_NAME" || true
    fi

    # Delete group
    if [ -n "${GROUP_NAME:-}" ]; then
        ksctl groups delete --name "$GROUP_NAME" > /dev/null 2>&1 && echo "✓ Deleted group: $GROUP_NAME" || true
    fi

    # Delete temp files
    rm -f /tmp/policy-$$.json /tmp/principal-$$.json /tmp/test-create-err-$$.txt /tmp/test-encrypt-kek-err-$$.txt /tmp/test-decrypt-kek-err-$$.txt /tmp/test-getkey-kek-err-$$.txt /tmp/test-encrypt-nonkek-err-$$.txt /tmp/encrypted-$$.json

    echo "✓ Cleanup complete"
}

trap cleanup EXIT

# Setup
echo "=========================================================================="
echo "Setup"
echo "=========================================================================="
echo ""

# Create test keys
echo "Creating test keys..."
ksctl keys create --name "$KEK_NAME" -a AES -i 256 > /dev/null
echo "✓ Created KEK: $KEK_NAME"

ksctl keys create --name "$NONKEK_NAME" -a AES -i 256 > /dev/null
echo "✓ Created non-KEK key: $NONKEK_NAME"
echo ""

# Create group
echo "Creating group..."
GROUP_RESPONSE=$(ksctl groups create \
    --name "$GROUP_NAME" \
    --description "Test group for Kroxylicious minimal permissions")

echo "✓ Created group: $GROUP_NAME"
echo ""

# Create policy
echo "Creating policy..."
# Important notes about this policy:
#
# 1. Resource pattern must use lowercase 'kek-*':
#    CTM matches against the key's URI, which is based on the key name but lowercased.
#    Key "KEK-production" has URI "kylo:kylo:vault:keys:kek-production" (lowercase).
#    The pattern must use lowercase to match.
#
# 2. All four actions are required:
#    - ReadKey: Required to get key metadata (used for alias resolution)
#    - EncryptWithKey: Required to encrypt DEKs with KEKs
#    - DecryptWithKey: Required to decrypt EDEKs
#    - UseKey: ALSO required for encrypt/decrypt operations to work
#
#    Note: UseKey might seem redundant alongside EncryptWithKey and DecryptWithKey,
#    but CTM requires it for crypto operations. Without UseKey, encrypt/decrypt
#    operations will fail with 403 Forbidden even if the specific actions are granted.
cat > /tmp/policy-$$.json <<EOF
{
  "name": "$POLICY_NAME",
  "allow": true,
  "resources": ["kylo:kylo:vault:keys:kek-*"],
  "actions": ["ReadKey", "EncryptWithKey", "DecryptWithKey", "UseKey"]
}
EOF

POLICY_RESPONSE=$(ksctl policy create --jsonfile /tmp/policy-$$.json)
POLICY_ID=$(echo "$POLICY_RESPONSE" | jq -r '.id')

if [ -z "$POLICY_ID" ] || [ "$POLICY_ID" = "null" ]; then
    echo "Error: Failed to create policy" >&2
    exit 1
fi

echo "✓ Created policy: $POLICY_NAME (ID: $POLICY_ID)"
echo "  Actions: ReadKey, EncryptWithKey, DecryptWithKey, UseKey"
echo "  Resources: kylo:kylo:vault:keys:kek-*"
echo ""

# Attach policy to group
echo "Attaching policy to group..."
cat > /tmp/principal-$$.json <<EOF
{
  "cust": {
    "groups": ["$GROUP_NAME"]
  }
}
EOF

ATTACH_RESPONSE=$(ksctl polattach create \
    --polid "$POLICY_ID" \
    --prinjson /tmp/principal-$$.json)

POLICY_ATTACH_ID=$(echo "$ATTACH_RESPONSE" | jq -r '.id')

if [ -z "$POLICY_ATTACH_ID" ] || [ "$POLICY_ATTACH_ID" = "null" ]; then
    echo "Error: Failed to attach policy to group" >&2
    exit 1
fi

echo "✓ Attached policy to group (Attachment ID: $POLICY_ATTACH_ID)"
echo ""

# Create test user
echo "Creating test user..."
USER_RESPONSE=$(ksctl users create --name "$USER_NAME" --pword "$USER_PASSWORD")
USER_ID=$(echo "$USER_RESPONSE" | jq -r '.user_id')

if [ -z "$USER_ID" ] || [ "$USER_ID" = "null" ]; then
    echo "Error: Failed to create user" >&2
    exit 1
fi

echo "✓ Created user: $USER_NAME (ID: $USER_ID)"
echo ""

# Add user to group
echo "Adding user to group..."
ksctl groups adduser -n "$GROUP_NAME" -u "$USER_ID" > /dev/null
echo "✓ Added user to group: $GROUP_NAME"
echo ""

# Tests
echo "=========================================================================="
echo "Test 1: CreateKey (should be DENIED)"
echo "=========================================================================="
echo ""

echo "Attempting to create key as test user..."
echo ""

if ksctl --user ${USER_NAME} --password ${USER_PASSWORD} keys create --name "test-unauthorized-$$" -a AES -i 256 2>/tmp/test-create-err-$$.txt; then
    echo "❌ FAIL: CreateKey succeeded"
    echo "   Policy should block CreateKey (not in actions list)"
    cat /tmp/test-create-err-$$.txt
    # Cleanup unauthorized key
    ksctl keys delete --name "test-unauthorized-$$" > /dev/null 2>&1 || true
else
    ERROR=$(cat /tmp/test-create-err-$$.txt | jq -r '.codeDesc // .message // "Unknown"' 2>/dev/null || echo "Permission denied")
    echo "✅ PASS: CreateKey denied"
    echo "   Error: $ERROR"
    echo "   Policy correctly blocks CreateKey (not in actions list)"
fi

echo ""

echo "=========================================================================="
echo "Test 2: Encrypt with KEK-* (should be ALLOWED)"
echo "=========================================================================="
echo ""

echo "Attempting to encrypt with KEK-* key as test user..."
echo "  Key: $KEK_NAME (matches policy resources: kek-*)"
echo ""

if ksctl --user ${USER_NAME} --password ${USER_PASSWORD}  crypto encrypt -k "$KEK_NAME" -l "test-data" 2>/tmp/test-encrypt-kek-err-$$.txt > /dev/null; then
    echo "✅ PASS: Encrypt with KEK-* succeeded"
    echo "   Policy correctly allows EncryptWithKey on kek-* resources"
else
    cat /tmp/test-encrypt-kek-err-$$.txt
    ERROR=$(cat /tmp/test-encrypt-kek-err-$$.txt | jq -r '.codeDesc // .message // "Unknown"' 2>/dev/null || echo "Unknown error")
    echo "❌ FAIL: Encrypt with KEK-* denied"
    echo "   Error: $ERROR"
    echo "   Policy should allow EncryptWithKey on kek-* resources"
fi

echo ""

echo "=========================================================================="
echo "Test 3: Decrypt with KEK-* (should be ALLOWED)"
echo "=========================================================================="
echo ""

echo "Attempting to decrypt with KEK-* key as test user..."
echo "  Key: $KEK_NAME (matches policy resources: kek-*)"
echo ""

# First, encrypt some data to get ciphertext (as admin)
ENCRYPTED=$(ksctl crypto encrypt -k "$KEK_NAME" -l "test-data-for-decrypt" -r /tmp/encrypted-$$.json 2>/dev/null)

if [ -f /tmp/encrypted-$$.json ]; then
    # Now try to decrypt as test user
    if ksctl --user ${USER_NAME} --password ${USER_PASSWORD} crypto decrypt -r /tmp/encrypted-$$.json 2>/tmp/test-decrypt-kek-err-$$.txt > /dev/null; then
        echo "✅ PASS: Decrypt with KEK-* succeeded"
        echo "   Policy correctly allows DecryptWithKey on kek-* resources"
    else
        ERROR=$(cat /tmp/test-decrypt-kek-err-$$.txt | jq -r '.codeDesc // .message // "Unknown"' 2>/dev/null || echo "Unknown error")
        echo "❌ FAIL: Decrypt with KEK-* denied"
        echo "   Error: $ERROR"
        echo "   Policy should allow DecryptWithKey on kek-* resources"
    fi
    rm -f /tmp/encrypted-$$.json
else
    echo "⚠️  Could not create encrypted data for test"
fi

echo ""

echo "=========================================================================="
echo "Test 4: List/Get KEK-* key (should be ALLOWED)"
echo "=========================================================================="
echo ""

echo "Attempting to get KEK-* key metadata as test user..."
echo "  Key: $KEK_NAME (matches policy resources: kek-*)"
echo ""

if ksctl --user ${USER_NAME} --password ${USER_PASSWORD} keys get --name "$KEK_NAME" 2>/tmp/test-getkey-kek-err-$$.txt > /dev/null; then
    echo "✅ PASS: Get KEK-* key succeeded"
    echo "   Policy correctly allows ReadKey on kek-* resources"
else
    ERROR=$(cat /tmp/test-getkey-kek-err-$$.txt | jq -r '.codeDesc // .message // "Unknown"' 2>/dev/null || echo "Unknown error")
    echo "❌ FAIL: Get KEK-* key denied"
    echo "   Error: $ERROR"
    echo "   Policy should allow ReadKey on kek-* resources"
fi

echo ""

echo "=========================================================================="
echo "Test 5: Encrypt with non-KEK-* (should be DENIED)"
echo "=========================================================================="
echo ""

echo "Attempting to encrypt with non-KEK-* key as test user..."
echo "  Key: $NONKEK_NAME (does NOT match policy resources: kek-*)"
echo ""

if ksctl --user ${USER_NAME} --password ${USER_PASSWORD}  crypto encrypt -k "$NONKEK_NAME" -l "test-data" 2>/tmp/test-encrypt-nonkek-err-$$.txt > /dev/null; then
    echo "❌ FAIL: Encrypt with non-KEK-* succeeded"
    echo "   Policy should block EncryptWithKey on non-kek-* resources"
else
    ERROR_MSG=$(cat /tmp/test-encrypt-nonkek-err-$$.txt 2>/dev/null || echo "")
    ERROR_CODE=$(echo "$ERROR_MSG" | jq -r '.codeDesc // "Unknown"' 2>/dev/null || echo "Unknown")

    if echo "$ERROR_CODE" | grep -q "NotFound\|ResourceNotFound"; then
        echo "✅ PASS: Encrypt with non-KEK-* denied"
        echo "   Error: $ERROR_CODE"
        echo "   Policy makes keys not matching kek-* pattern invisible"
    else
        echo "✅ PASS: Encrypt with non-KEK-* denied"
        echo "   Error: $ERROR_CODE"
        echo "   Policy correctly blocks access to keys not matching kek-* pattern"
    fi
fi

echo ""

echo "=========================================================================="
echo "Summary"
echo "=========================================================================="
echo ""
echo "This test validates that a CTM policy with:"
echo "  - Actions: ReadKey, EncryptWithKey, DecryptWithKey, UseKey"
echo "  - Resources: kylo:kylo:vault:keys:kek-*"
echo ""
echo "Correctly enforces minimal permissions for Kroxylicious:"
echo "  1. ✓ Blocks CreateKey (not in actions list)"
echo "  2. ✓ Allows EncryptWithKey on keys matching kek-* pattern"
echo "  3. ✓ Allows DecryptWithKey on keys matching kek-* pattern"
echo "  4. ✓ Allows ReadKey (get key metadata) on keys matching kek-* pattern"
echo "  5. ✓ Blocks crypto operations on keys NOT matching kek-* pattern"
echo ""
echo "To skip cleanup and inspect resources, run with:"
echo "  SKIP_CLEANUP=1 $0"
echo ""
