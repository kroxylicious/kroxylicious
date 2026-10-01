# Kroxylicious HashiCorp KMS Provider for Record Encryption

## What is it?

An implementation of the KMS (Key Management System) interface backed by a remote instance of Hashicorp Vault (v1).

It allows the Record Encryption filter to be used with encryption keys stored with a HashiCorp Vault(TM)
instance.  The instance may be a locally deployed instance, or could be a HashiCorp Vault instance provided by 
HashiCorp Cloud Platform (HCP) .

## How do I use it?

See the record encryption book with the Kroxylicious documentation.

## Authentication Methods

This KMS provider supports two different methods for authenticating with HashiCorp Vault:

### Static Token (`vaultToken`)
A static, long-lived Vault token provided either inline or via a file.

#### Example Configuration
```yaml
filterDefinitions:
  - name: my-encryption-filter
    type: RecordEncryption
    config:
      kms: VaultKmsService
      kmsConfig:
        vaultTransitEngineUrl: https://vault:8200/v1/transit
        vaultToken:
          passwordFile: /opt/vault/token
      selector: TemplateKekSelector
      selectorConfig:
        template: "KEK_$(topicName)"
```

### Kubernetes Service Account (`role`)
Authenticates to Vault using a Kubernetes Service Account JWT. This method requires Vault to have the [Kubernetes Auth Method](https://developer.hashicorp.com/vault/docs/auth/kubernetes) enabled and configured.
The provider will automatically read the JWT from the container, exchange it for a Vault client token, and handle token renewal before it expires.

#### Example Configuration
```yaml
filterDefinitions:
  - name: my-encryption-filter
    type: RecordEncryption
    config:
      kms: VaultKmsService
      kmsConfig:
        vaultTransitEngineUrl: https://vault.default.svc.cluster.local:8200/v1/transit
        # The Vault role bound to your Kubernetes ServiceAccount (Required for Kubernetes Auth)
        role: "kroxylicious-vault-role"
        
        # Optional parameters and their defaults:
        # serviceAccountTokenPath: "/var/run/secrets/kubernetes.io/serviceaccount/token"
        # authPath: "kubernetes"
      selector: TemplateKekSelector
      selectorConfig:
        template: "KEK_$(topicName)"
```

*Note: The `vaultToken` and `role` configuration properties are mutually exclusive.*

HashiCorp, HashiCorp Cloud Platform and HashiCorp Vault are registered trademarks of HashiCorp Inc.

