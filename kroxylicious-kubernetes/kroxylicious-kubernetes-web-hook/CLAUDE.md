# Kroxylicious Sidecar Webhook - Claude Context

This module implements a Kubernetes mutating admission webhook for sidecar injection.

## Primary Documentation

See [README.md](README.md) for comprehensive webhook documentation including:
- Trust model (admin vs app owner)
- Injection decision logic
- Delegated annotations
- Native sidecar support (K8s 1.28+)
- Fail-open semantics
- Key classes and configuration

## Critical Constraints

**Fail-open**: The webhook must always return `allowed: true`. Never reject a pod admission request, even on internal errors.

**Trust model**: The webhook administrator does not trust the application pod owner. The webhook always overwrites the proxy config annotation. Non-delegated annotations in the `kroxylicious.io/` namespace are ignored.

**Security context**: The sidecar security context is never weakened. The proxy runs as non-root with read-only root filesystem and all capabilities dropped.

## For Claude

When working with this module:
- Follow fail-open semantics in error handling
- Never allow app owners to control the proxy image, security context, or upstream address
- Delegated annotations must be explicitly listed in `KroxyliciousSidecarConfig`
- The proxy config YAML is generated using the same `Configuration` model from `kroxylicious-runtime`
- Use structured logging with `addKeyValue()` per the [logging rules](../.claude/rules/logging.md)

See [README.md](README.md) for detailed guidance.
