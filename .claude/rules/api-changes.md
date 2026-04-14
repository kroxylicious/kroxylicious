---
paths:
  - "**/*.{java}"
---

# API Change Requirements

## What Requires Proposal Process

Changes to public APIs require a formal proposal process and community agreement.

## Public API Scope

**Java APIs (require proposals for non-trivial changes):**
- `kroxylicious-api` - All filter APIs and plugin interfaces
- `kroxylicious-authorizer-api` - Authorisation plugin API
- `kroxylicious-kms` - Key management system API
- Anything an end user or plugin developer is expected to touch

**Kubernetes APIs (require proposals following Kubernetes versioning):**
- All CRDs in `kroxylicious-kubernetes-api`
- Must follow Kubernetes API versioning (v1alpha1, v1beta1, v1)
- Backwards compatibility guarantees per Kubernetes conventions

**CLI and Configuration:**
- CLI interface of the proxy
- YAML configuration syntax (must remain compatible with existing configs)
- Operator environment variables and command-line arguments

## Trivial Changes (No Proposal Required)

- Fixing typos in documentation or Javadoc
- Clarifying existing Javadoc without changing semantics
- Adding internal implementation details (in non-API modules)

## Implementation Changes

Changes to non-API modules (e.g., `kroxylicious-runtime`) generally don't require proposals, but:
- Java classes representing configuration are NOT public API
- But YAML syntax IS public API (backwards compatibility required)
- Implementation can change if YAML remains compatible

## Why This Matters

Plugin interfaces need to be thoroughly documented and understandable by reasonably competent Java developers. Breaking changes impact end users who have written their own plugin implementations.
