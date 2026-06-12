#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Replace CipherTrust Manager server certificate with a self-signed certificate
# containing custom DNS names.
# Intended for test use only.
#
# Usage: replace-server-cert.sh <hostname> [additional-dns-names...]
#
# Example:
#   replace-server-cert.sh ec2-34-241-225-83.eu-west-1.compute.amazonaws.com
#   replace-server-cert.sh example.com *.example.com additional.example.com
#

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <hostname> [additional-dns-names...]" >&2
    echo "" >&2
    echo "Example:" >&2
    echo "  $0 ec2-34-241-225-83.eu-west-1.compute.amazonaws.com" >&2
    echo "  $0 example.com *.example.com additional.example.com" >&2
    exit 1
fi

# Primary hostname (used as CN)
PRIMARY_HOSTNAME="$1"
shift

# Build DNS names array
DNS_NAMES=("$PRIMARY_HOSTNAME")
if [ $# -gt 0 ]; then
    DNS_NAMES+=("$@")
fi

# Check ksctl is available
if ! command -v ksctl &> /dev/null; then
    echo "Error: ksctl command not found. Please install it first." >&2
    exit 1
fi

# Check if logged in by attempting to list interfaces
if ! ksctl interfaces list &> /dev/null; then
    echo "Error: Not logged in to CipherTrust Manager. Please login first:" >&2
    echo "  ksctl login --url https://<hostname> --user admin --nosslverify" >&2
    exit 1
fi

# Create temporary file for auto-gen attributes
TEMP_JSON=$(mktemp)
trap "rm -f $TEMP_JSON" EXIT

# Build DNS names JSON array
DNS_NAMES_JSON="["
first=true
for dns in "${DNS_NAMES[@]}"; do
    if [ "$first" = true ]; then
        first=false
    else
        DNS_NAMES_JSON+=","
    fi
    DNS_NAMES_JSON+="\"$dns\""
done
DNS_NAMES_JSON+="]"

# Create auto-gen attributes JSON
cat > "$TEMP_JSON" <<EOF
{
  "cn": "$PRIMARY_HOSTNAME",
  "dns_names": $DNS_NAMES_JSON,
  "names": [
    {
      "C": "US",
      "O": "kroxylicious.io",
      "OU": "dev"
    }
  ]
}
EOF

echo "==> Updating web interface auto-generation attributes..."
ksctl interfaces modify --name web --local-auto-gen-attributes-file "$TEMP_JSON"

echo ""
echo "==> Generating new server certificate..."
ksctl interfaces auto-gen-server-cert --name web

echo ""
echo "==> Restarting web service to activate new certificate..."
ksctl services restart --service-names web -y

echo ""
echo "==> Waiting for service to restart (10 seconds)..."
sleep 10

echo ""
echo "==> Certificate replacement complete!"
echo ""
echo "You can verify the certificate with:"
echo "  echo | openssl s_client -connect $PRIMARY_HOSTNAME:443 -servername $PRIMARY_HOSTNAME 2>&1 | openssl x509 -noout -subject -ext subjectAltName"
