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

## Design Proposals Repository

**Location:** https://github.com/kroxylicious/design/tree/main/proposals

**When Planning API Changes or Significant Features:**
1. Check if a relevant approved proposal exists in the design repo
2. Use WebFetch to retrieve proposal content from GitHub:
   - Raw URL format: `https://raw.githubusercontent.com/kroxylicious/design/main/proposals/<proposal-name>.md`
   - Browse format: `https://github.com/kroxylicious/design/blob/main/proposals/<proposal-name>.md`
3. Validate that your implementation plan aligns with the approved proposal
4. If no proposal exists but the change requires one (see scope above), inform the user that a proposal must be created first

**How to Find Relevant Proposals:**
- Use WebFetch on `https://github.com/kroxylicious/design/tree/main/proposals` to see the list
- Look for proposals related to the API module, feature name, or issue number
- Check the proposals directory README for indexing information

**Check for In-Flight Proposals:**
- New design proposals may be under review as open PRs in the design repository
- Use `gh pr list --repo kroxylicious/design` or WebFetch on `https://github.com/kroxylicious/design/pulls` to see open proposals
- Check both the PR title/description and linked proposal files for relevance
- For open proposals, note that the design is not yet finalized and may change during review

**Important:** 
- Approved proposals are the authoritative design. Implementation must follow the proposal unless there's a documented reason to deviate (which requires discussion).
- **Proposals are numbered sequentially. When multiple proposals exist for related functionality, higher-numbered proposals take precedence.** A later proposal may supersede, extend, or countermand an earlier one.
- If proposals appear to conflict, the one with the higher number is authoritative.

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
