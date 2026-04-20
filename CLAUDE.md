# Kroxylicious - Claude Context

This is Kroxylicious, a Layer 7 proxy for the Kafka protocol. It's written using Netty and uses the same `*RequestData` and `*ResponseData` classes which Apache Kafka does to represent protocol messages.

## Primary Documentation

See [README.md](README.md) for comprehensive project documentation including:
- Architecture and design (Netty pipeline, filter system, protocol contracts)
- User personas
- Security model
- Deployment considerations
- Configuration
- API vs implementation distinctions
- Testing guidance

## Build and Development

See [DEV_GUIDE.md](DEV_GUIDE.md) for:
- Build commands and Maven properties/profiles
- Maven profiles: `qa` (quality checks), `ci`, `dist`, `quick`, `systemtest`, `errorprone-jdk-compatible`
- IDE setup and debugging
- Running integration/system tests locally
- Container image building and pushing
- Continuous integration workflows

## Coding Rules

When writing code, follow these prescriptive rules:
- [API Changes](.claude/rules/api-changes.md) - When proposals are required
- [Performance](.claude/rules/performance.md) - Performance-sensitive patterns
- [Security](.claude/rules/security-patterns.md) - Security coding requirements
- [Logging](.claude/rules/logging.md) - Logging conventions
- [Documentation](.claude/rules/documentation-requirements.md) - When to write docs

## Module-Specific Context

See module README.md files for detailed context:
- [kroxylicious-api/README.md](kroxylicious-api/README.md) - Filter API
- [kroxylicious-runtime/README.md](kroxylicious-runtime/README.md) - Proxy runtime
- [kroxylicious-filters/README.md](kroxylicious-filters/README.md) - End-user filters
- [kroxylicious-kms/README.md](kroxylicious-kms/README.md) - KMS API
- [kroxylicious-authorizer-api/README.md](kroxylicious-authorizer-api/README.md) - Authoriser API
- [kroxylicious-operator/README.md](kroxylicious-operator/README.md) - Kubernetes operator
- [kroxylicious-kubernetes-api/README.md](kroxylicious-kubernetes-api/README.md) - Kubernetes CRDs
- [kroxylicious-docs/README.md](kroxylicious-docs/README.md) - Documentation

## Kubernetes

There is a Kubernetes operator for the proxy in `kroxylicious-operator`. The end user defines a number of custom resources, such as `KafkaProxy`, `KafkaProxyIngress`, and `KafkaProtocolFilter`, and the operator observes ("reconciles") these and creates/updates a Kubernetes `Deployment` to run a number of proxy instances as containers within `Pods`.

## End User Documentation

End user documentation lives in `kroxylicious-docs`.

