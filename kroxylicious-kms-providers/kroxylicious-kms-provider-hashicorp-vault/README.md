# Kroxylicious HashiCorp KMS Provider for Record Encryption

## What is it?

An implementation of the KMS (Key Management System) interface backed by a remote instance of Hashicorp Vault (v1).

It allows the Record Encryption filter to be used with encryption keys stored with a HashiCorp Vault(TM)
instance.  The instance may be a locally deployed instance, or could be a HashiCorp Vault instance provided by 
HashiCorp Cloud Platform (HCP) .

## How do I use it?

See the record encryption book with the Kroxylicious documentation.

## Config2 (Versioned Configuration)

`VaultKmsService` supports both legacy and versioned (config2) configuration via dual `@Plugin` annotations:

- **Legacy** (`Config`): Inline `Tls` object with embedded key/trust provider definitions and `PasswordProvider` for the vault token.
- **v1** (`VaultKmsConfigV1`): References `KeyMaterialProvider` and `TrustMaterialProvider` plugin instances by name via `HasPluginReferences`. The vault token is a plain string (sourced from a Kubernetes Secret or restricted-permission config file in production).

A v1 JSON Schema is at `META-INF/kroxylicious/schemas/VaultKmsService/v1.schema.yaml`.

HashiCorp, HashiCorp Cloud Platform and HashiCorp Vault are registered trademarks of HashiCorp Inc.

