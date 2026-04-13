# Public APIs

This document enumerates every API in the Kroxylicious project that is considered Public and subject to the [Deprecation Policy](DEV_GUIDE.md#deprecation-policy).
These APIs follow semantic versioning principles.

## Table of Contents

<!-- TOC -->
* [Public APIs](#public-apis)
  * [Table of Contents](#table-of-contents)
  * [1. Java APIs](#1-java-apis)
  * [2. Kubernetes APIs (Custom Resource Definitions)](#2-kubernetes-apis-custom-resource-definitions)
    * [2.1 CRDs (v1alpha1)](#21-crds-v1alpha1)
  * [3. Command Line Interface](#3-command-line-interface)
    * [3.1 Kroxylicious Proxy CLI](#31-kroxylicious-proxy-cli)
  * [4. Shell Script APIs](#4-shell-script-apis)
    * [4.1 Proxy Startup Script](#41-proxy-startup-script)
  * [5. Configuration File Format](#5-configuration-file-format)
  * [6. First-Party Filter Configurations](#6-first-party-filter-configurations)
  * [7. KMS Provider Configurations](#7-kms-provider-configurations)
  * [8. Authorizer Provider Configurations](#8-authorizer-provider-configurations)
  * [9. Management API](#9-management-api)
  * [Notes](#notes)
<!-- TOC -->

## 1. Java APIs

The public types of these maven modules are considered public API, third party Authors depend on them to integrate
with Kroxylicious.

| Module                      | Coordinates                                 |
|-----------------------------|---------------------------------------------|
| kroxylicious-api            | io.kroxylicious:kroxylicious-api            |
| kroxylicious-annotations    | io.kroxylicious:kroxylicious-annotations    |
| kroxylicious-kms            | io.kroxylicious:kroxylicious-kms            |
| kroxylicious-authorizer-api | io.kroxylicious:kroxylicious-authorizer-api |

## 2. Kubernetes APIs (Custom Resource Definitions)

Location: `kroxylicious-kubernetes-api/src/main/resources/META-INF/fabric8/`

The Kubernetes CRDs define the declarative API for operating Kroxylicious on Kubernetes. The schema structure defined in the YAML files and the corresponding Java model classes are public APIs.

### 2.1 CRDs (v1alpha1)

All CRDs are in API group `kroxylicious.io` with version `v1alpha1`.

- **KafkaProxy** (`kafkaproxies.kroxylicious.io`)
- **VirtualKafkaCluster** (`virtualkafkaclusters.kroxylicious.io`)
- **KafkaService** (`kafkaservices.kroxylicious.io`)
- **KafkaProtocolFilter** (`kafkaprotocolfilters.kroxylicious.io`)
- **KafkaProxyIngress** (`kafkaproxyingresses.kroxylicious.io`)

## 3. Command Line Interface

The main method available to Users of the binary distribution is Public API. Any arguments
or Environment Variables they support are part of the contract between the User and Proxy.

### 3.1 Kroxylicious Proxy CLI

Main class: `io.kroxylicious.app.Kroxylicious`
**Options:**
- `-c, --config <FILE>` - Path to configuration file (required)
- `-h, --help` - Show help message and exit
- `-V, --version` - Print version information and exit

**Exit Codes:**
- `0` - Successful execution
- Non-zero - Error occurred

**Version Information Format:**
```
kroxylicious: <version>
commit id: <git-commit-id>
```

## 4. Shell Script APIs

The shell scripts available to Users of the binary distribution are Public API. Any arguments
or Environment Variables they support are part of the contract between the User and Proxy.

### 4.1 Proxy Startup Script

Script: `bin/kroxylicious-start.sh`

## 5. Configuration File Format

The YAML configuration file structure is considered a public API. Changes to configuration schema must follow the deprecation policy.

## 6. First-Party Filter Configurations

Kroxylicious maintains the following first-party filters in the `kroxylicious-filters` module. The configuration schema for each filter is part of the public API.

**Filter Modules:**
- `kroxylicious-authorization` - Authorization filter
- `kroxylicious-connection-expiration` - Connection expiration management
- `kroxylicious-entity-isolation` - Entity isolation and multi-tenancy
- `kroxylicious-multitenant` - Multi-tenant support
- `kroxylicious-oauthbearer-validation` - OAuth bearer token validation
- `kroxylicious-record-encryption` - Record-level encryption
- `kroxylicious-record-validation` - Record validation
- `kroxylicious-sasl-inspection` - SASL authentication inspection
- `kroxylicious-simple-transform` - Simple record transformation

Each filter's configuration class (referenced in its `@Plugin` annotation) defines the public configuration API for that filter.

## 7. KMS Provider Configurations

The following KMS providers are maintained as first-party implementations in the `kroxylicious-kms-providers` module. The configuration schema for each provider is part of the public API.

**KMS Provider Modules:**
- `kroxylicious-kms-provider-aws-kms` - AWS Key Management Service
  - Configuration class: `io.kroxylicious.kms.provider.aws.kms.config.Config`
- `kroxylicious-kms-provider-azure-key-vault-kms` - Azure Key Vault
  - Configuration class: `io.kroxylicious.kms.provider.azure.keyvault.config.Config`
- `kroxylicious-kms-provider-fortanix-dsm` - Fortanix Data Security Manager
  - Configuration class: `io.kroxylicious.kms.provider.fortanix.dsm.config.Config`
  - Session provider configs:
    - `io.kroxylicious.kms.provider.fortanix.dsm.config.ApiKeySessionProviderConfig`
- `kroxylicious-kms-provider-hashicorp-vault` - HashiCorp Vault
  - Configuration class: `io.kroxylicious.kms.provider.hashicorp.vault.config.Config`
- `kroxylicious-kms-provider-kroxylicious-inmemory` - In-memory KMS (for testing)
  - Configuration class: `io.kroxylicious.kms.provider.kroxylicious.inmemory.Config`

## 8. Authorizer Provider Configurations

The following authorizer providers are maintained as first-party implementations in the `kroxylicious-authorizer-providers` module. The configuration schema for each provider is part of the public API.

**Authorizer Provider Modules:**
- `kroxylicious-authorizer-acl` - ACL-based authorization
  - Configuration class: `io.kroxylicious.authorizer.acl.config.AclAuthorizer.Config`

## 9. Management API

The Proxy's (optional) management endpoint offers prometheus formatted metrics at `/metrics`

---

## Notes

1. **Module Coordinates:** All modules use the Maven group ID `io.kroxylicious` and follow the naming pattern `kroxylicious-<module-name>`.

2. **API Stability:** All APIs listed in this document are subject to the [Deprecation Policy](DEV_GUIDE.md#deprecation-policy).

3. **Version Indicators:** The Kubernetes CRDs are currently at version `v1alpha1`, indicating pre-1.0 stability. The Java APIs follow the overall project version.

4 **Filter Development:** The `kroxylicious-filter-archetype` Maven archetype can be used to create new filters but is not itself a public API. The archetype may change between versions.

5 **Test Support Modules:** Modules ending in `-test-support` are not public APIs and may change without notice.
