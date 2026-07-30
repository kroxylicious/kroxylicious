# Kroxylicious Admission API - Claude Context

This module defines the `KroxyliciousSidecarConfig` CRD for the Kroxylicious sidecar injection webhook.

## Primary Documentation

See [README.md](README.md) for comprehensive API documentation including:
- API roles (CR Authors, Webhook Developers)
- `KroxyliciousSidecarConfig` CRD fields and status conditions
- Kubernetes API versioning and compatibility
- Gateway API inspiration for status conditions

## Critical Constraints

**Public API:** The CRD YAML schemas in this module are a public API subject to Kubernetes API versioning and compatibility requirements.

**Generated Java:** The Java classes are generated from CRD schemas - do NOT edit directly. See [../.claude/rules/api-changes.md](../.claude/rules/api-changes.md).

## For Claude

When working with this module:
- CRD changes require proposal process
- Follow Kubernetes API conventions
- Maintain backwards compatibility
- Update status conditions following Gateway API patterns

See [README.md](README.md) for detailed guidance.
