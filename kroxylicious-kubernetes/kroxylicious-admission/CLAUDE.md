# Kroxylicious Sidecar Webhook - Claude Context

This module implements a Kubernetes mutating admission webhook for sidecar injection.

## Primary Documentation

See [README.md](README.md) for comprehensive webhook documentation including:
- Trust model (admin vs app owner)
- Injection decision logic
- Native sidecar support (K8s 1.28+)
- Failure policy and uninjected pod policy
- Key classes and configuration

## Critical Constraints

**Fail-closed by default**: Internal errors cause HTTP 500; the `MutatingWebhookConfiguration` uses `failurePolicy: Fail` to block pod creation when the webhook is unreachable or errors. Non-error skip paths (null request, null pod, opt-out, already injected) always return `allowed: true`.

**Uninjected pod policy**: The `UNINJECTED_POD_POLICY` environment variable (`Admit` or `Deny`) controls whether pods that cannot be injected (no config, multiple configs, invalid config) are admitted or denied.

**Trust model**: The webhook administrator does not trust the application pod owner. The webhook always overwrites the `sidecar.kroxylicious.io/proxy-config` annotation.

**Security context**: The sidecar security context is never weakened. The proxy runs as non-root with read-only root filesystem and all capabilities dropped.

## For Claude

When working with this module:
- Internal errors are thrown as exceptions, causing HTTP 500 (handled by MWC failurePolicy)
- `denyUninjected` flag controls whether config-unavailable pods are denied or admitted
- Never allow app owners to control the proxy image, security context, or target cluster address
- The proxy config YAML is generated using the same `Configuration` model from `kroxylicious-runtime`
- Use structured logging with `addKeyValue()` per the [logging rules](../.claude/rules/logging.md)

See [README.md](README.md) for detailed guidance.
