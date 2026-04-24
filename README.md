# Kroxylicious

Kroxylicious, the snappy open source proxy for [Apache Kafka®](https://kafka.apache.org).

Kroxylicious is a Kafka protocol proxy, addressing use cases such as encryption, multi-tenancy and schema validation.

<!-- TOC -->
* [Kroxylicious](#kroxylicious)
  * [Quick Links](#quick-links)
  * [Build status](#build-status)
  * [License](#license)
  * [Developer Guide](#developer-guide)
  * [Releasing this project](#releasing-this-project)
  * [Performance Testing](#performance-testing)
  * [Kroxylicious Filter Development](#kroxylicious-filter-development)
  * [Contributing](#contributing)
  * [Architecture](#architecture)
    * [Filter System](#filter-system)
    * [Plugin System](#plugin-system)
    * [Protocol Contracts](#protocol-contracts)
  * [User Personas](#user-personas)
  * [Security Model](#security-model)
    * [Deployment Threat Model](#deployment-threat-model)
    * [Authentication Architecture](#authentication-architecture)
    * [Authorisation Patterns](#authorisation-patterns)
    * [TLS/Transport Security](#tlstransport-security)
  * [Deployment Considerations](#deployment-considerations)
  * [Configuration](#configuration)
  * [API vs Implementation](#api-vs-implementation)
  * [Testing](#testing)
<!-- TOC -->

## Quick Links
- [kroxylicious.io](https://www.kroxylicious.io)
- [Documentation](https://www.kroxylicious.io/kroxylicious)
- [GitHub design and discussion](https://github.com/kroxylicious/design)
- [Community Slack chat](https://kroxylicious.slack.com/)

## Build status
![Maven Central Version](https://img.shields.io/maven-central/v/io.kroxylicious/kroxylicious-parent)
 [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=kroxylicious_kroxylicious&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=kroxylicious_kroxylicious) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=kroxylicious_kroxylicious&metric=coverage)](https://sonarcloud.io/summary/new_code?id=kroxylicious_kroxylicious)

## License

This code base is available under the [Apache License, version 2](LICENSE).

## Developer Guide

See the [Developer Guide](DEV_GUIDE.md).

## Releasing this project

See the [Release Guide](RELEASING.md)

## Performance Testing

See the [Performance Guide](PERFORMANCE.md) for information on running basic performance tests for this proxy.

## Kroxylicious Filter Development

Use [kroxylicious-filter-archetype](kroxylicious-filter-archetype) to get started developing a Custom Filter.

Run `mvn archetype:generate -DarchetypeGroupId=io.kroxylicious -DarchetypeArtifactId=kroxylicious-filter-archetype` to generate a standalone Filter project.

## Contributing

We welcome contributions! Please see our [contributing guidelines](https://github.com/kroxylicious/.github/blob/main/CONTRIBUTING.md) to get started.

If you think you have found a security-related issue with Kroxylicious then please follow the [SECURITY process](https://github.com/kroxylicious/.github/blob/main/SECURITY.md).

Our [GOVERNANCE.md](https://github.com/kroxylicious/.github/blob/main/GOVERNANCE.md) document describes how the project is run, including how decisions are made, and by whom.

## Architecture

At its core, the proxy uses the same setup as the proxy example code from the Netty project.

`KafkaProxyInitializer` sets up the pipeline when a Kafka client connects. `KafkaProxyFrontendHandler` reads requests from the client and writes responses to the client. `KafkaProxyBackendHandler` writes requests to the broker and reads responses. In the middle of the frontend pipeline is one or more `io.kroxylicious.proxy.internal.FilterHandler` instances.

### Filter System

A `FilterHandler` functions as an adapter for a "filter plugin". Filter plugins are concrete classes implementing one of the subinterfaces of `io.kroxylicious.proxy.filter.Filter`:

- `io.kroxylicious.proxy.filter.RequestFilter` and `io.kroxylicious.proxy.filter.ResponseFilter` can receive all Kafka protocol messages
- Kafka API-specific interfaces for each Kafka protocol API, like `MetadataRequestFilter` or `FetchResponseFilter`

Filters are instantiated using a `FilterFactory`. The project includes a number of filter implementations. Many are used for testing purposes, but the ones in `kroxylicious-filters` are intended to be used by end users.

### Plugin System

The proxy allows other plugins beyond filters. See `TransportSubjectBuilderService` and `SaslSubjectBuilderService` as examples. Plugins can have plugins of their own. For example, the `RecordEncryption` filter uses a `Kms` (key management system), which is a plugin with a number of implementations in the project. Similarly, the `Authorisation` filter uses an `Authoriser` which is a plugin.

All plugins are loaded using the normal Java `java.util.ServiceLoader` mechanism. We expect and support end users writing their own plugin implementations.

### Protocol Contracts

The proxy must maintain Kafka protocol semantics and guarantees:

**Request-Response Ordering:**
- `ResponseOrderer` ensures FIFO response ordering per connection, even when filters process messages asynchronously
- Uses correlation IDs to match requests with responses
- Queues out-of-order responses until earlier ones arrive

**Pipelining Support:**
- Kafka clients can send multiple requests before receiving responses (pipelining)
- Brokers may respond out-of-order
- `FilterHandler` chains async operations to maintain per-connection ordering
- **Critical constraint:** Filters must NOT assume responses arrive before all requests are sent

**Backpressure Propagation:**
- Dual state machine tracks both session state AND backpressure state
- When the broker pauses (TCP write buffer full), the proxy must pause the client
- When the client pauses, the proxy must pause the broker
- Filters must not buffer indefinitely or break the pause/resume cycle

**Connection-Level Semantics:**
- All protocol guarantees are per-connection only
- No cross-connection state assumptions
- Each proxy instance handles independent connections

**Key Implementation Files:**
- `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/ResponseOrderer.java`
- `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/FilterHandler.java`
- `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/ProxyChannelStateMachine.java`
- `kroxylicious-api/src/main/java/io/kroxylicious/proxy/package-info.java`

## User Personas

**Filter Plugin Developer:** Writes custom filters for data transformation, validation, or enrichment. Needs to understand the Filter API, protocol contracts, threading model, and testing patterns. Must work within filter constraints to avoid breaking Kafka semantics.

**Security Plugin Developer:** Implements custom authentication (`SaslSubjectBuilder`/`TransportSubjectBuilder`) or authorisation (`Authoriser`) plugins. Requires deep understanding of the security model, fail-safe patterns, and Subject/Principal composition.

**KMS Plugin Developer:** Implements integration with external key management systems. Needs to understand DEK (Data Encryption Key) lifecycle, resilience patterns (exponential backoff, caching), and async error handling.

**Proxy Operations Engineer:** Deploys and maintains the proxy day-to-day. Responsible for ensuring the proxy is running, monitoring its health, managing deployments (bare metal, containers, Kubernetes), and responding to operational issues.

**Compliance/Security Auditor:** Verifies that the proxy meets security and compliance requirements. Needs to confirm TLS versions and cipher suites in use, understand which KEKs (Key Encryption Keys) are configured, validate authentication and authorisation settings, and ensure audit logging captures required events.

**Kafka Application Developer:** Writes Kafka clients (producers/consumers) that connect through the proxy. Often unaware of the proxy's presence, but may need to understand how certain filters affect their application (e.g., record validation filters may reject malformed messages, record encryption changes payload size).

## Security Model

### Deployment Threat Model

- Proxy instances are independent processes with no shared state
- Cannot assume interception of all client-broker communication (distributed deployment)
- Implications: stateful security decisions require central coordination or cache invalidation strategies
- Must handle untrusted input from both clients and potentially compromised Kafka cluster administrators

### Authentication Architecture

- Two-stage authentication: transport-level (mTLS) occurs first, then SASL authentication
- Subject construction uses Principal composition (e.g., `User`, `@Unique` principals)
- Anonymous fallback on authentication failure (never throw exceptions that close connections unexpectedly)
- `TransportSubjectBuilder` builds subjects from TLS context before any requests
- `SaslSubjectBuilder` builds subjects from SASL context after successful authentication

### Authorisation Patterns

- Fail-closed semantics: deny by default, only allow explicitly permitted actions
- Resource type validation prevents silent failures where policies cannot be enforced
- Async authorisation decision points via `CompletionStage<AuthoriseResult>`
- Current authorisation filter is experimental (production warnings in logging)

### TLS/Transport Security

- Dual-role TLS: proxy acts as both server (to clients) and client (to brokers)
- Certificate chain handling (certificate + optional intermediates)
- Trust store configuration for custom CAs
- Cipher suite and protocol allow-lists and deny-lists

For detailed security coding patterns, see [.claude/rules/security-patterns.md](.claude/rules/security-patterns.md).

## Deployment Considerations

The proxy is often deployed as a number of independent processes. They don't know about each other. This has consequences in terms of some of the things a `Filter` can safely do. In particular, a Filter cannot assume other connections to the same cluster are happening through the same proxy process. Thus a filter instance cannot assume it is intercepting all of the client-to-broker communication.

## Configuration

A proxy instance is configured using a YAML configuration file. The plugins to be used will be referred to in that YAML file, usually by fully-qualified or unqualified class name. Plugins usually accept YAML configuration of their own, which will be part of the main YAML configuration file. Sometimes the YAML configuration file will refer by name to other files which are also considered part of the proxy's configuration. For example, this is used for security-sensitive configuration such as TLS keys and certificates, and also for the `AclAuthoriser`'s rules file.

## API vs Implementation

We make a clear distinction between "public API" and implementation. "Public API" is anything an end user or plugin developer is expected to touch:

- All the plugin Java APIs (anything in `kroxylicious-api`, `kroxylicious-authorizer-api`, `kroxylicious-kms`)
- The CLI interface of the proxy itself
- The `CustomResourceDefinitions` for the Kubernetes operator in `kroxylicious-kubernetes-api`

**Important:** Changes to public APIs require a formal proposal process. See [.claude/rules/api-changes.md](.claude/rules/api-changes.md) for details.

Other modules, such as `kroxylicious-runtime`, are implementation. These can be changed as necessary, but even then the situation is not always clear. Specifically, while the Java classes that are the Java representation of the parsed configuration file are not a public Java API, the YAML configuration syntax itself _is_. So those classes can only be changed in a way that remains compatible with already existing configuration files which users might have written.

## Testing

- **`kroxylicious-filter-test-support`**: Utilities for unit testing filters in isolation
- **`kroxylicious-integration-test-support`**: Infrastructure for integration tests with real Kafka clusters
- **Example patterns**: Study existing filter tests in `kroxylicious-filters` modules for patterns
