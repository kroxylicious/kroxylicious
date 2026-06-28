#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Minimal permissions required by Envelope Encryption.

path "transit/keys/*" {
capabilities = ["read"]
}
path "/transit/datakey/plaintext/*" {
capabilities = ["update"]
}
path "transit/decrypt/*" {
capabilities = [ "update"]
}