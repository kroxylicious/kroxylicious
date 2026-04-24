---
paths:
  - "**/*"
---

# Commit Message Conventions

## Format

Use Conventional Commits with AI disclosure.

**Structure:**
```
<type>(<scope>): <subject>

<body>

<trailers>
```

**Types:** feat, fix, docs, build, chore, refactor, perf, test, ci

**Scope** (optional): Module or area (e.g., aws-kms, filters, runtime)

**Subject:** Imperative mood, no period, <72 chars

## AI Disclosure

**Required:** When AI assists with code changes, add `Assisted-by:` trailer.

**Format:** `Assisted-by: Claude <model-name> <noreply@anthropic.com>`

**Placement:** After body, before Signed-off-by

**Model names:**
- Claude Sonnet 4.5
- Claude Sonnet 4.6
- Claude Opus 4.6
- (Use actual model name from session)

## Trailers Order

1. Other trailers (Relates-to:, Fixes:, etc.)
2. `Assisted-by:` (if applicable)
3. `Signed-off-by:` (added automatically by git hook)

## DCO Signoff

The prepare-commit-msg hook automatically adds `Signed-off-by:` trailer. Do not add manually.

## Complete Example

```
feat(record-encryption): add key rotation support

Implements automatic KEK rotation with configurable intervals and
zero-downtime key migration. Includes metrics for rotation events.

Relates-to: #1234
Assisted-by: Claude Sonnet 4.5 <noreply@anthropic.com>
Signed-off-by: Developer Name <dev@example.com>
```

## Important Notes

- **Do NOT use** `Co-Authored-By:` for AI disclosure - use `Assisted-by:`
- The git hook adds Signed-off-by automatically - you don't need to add it
- When Claude Code creates commits, ensure it uses `Assisted-by:` format
