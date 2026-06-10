---
paths:
  - "**/*Test.java"
  - "**/*IT.java"
---

# Test Structure Conventions

## Given / When / Then

Structure test bodies using three labelled sections:

- **Given** — the state required for the action to be meaningful. This is setup: initialising
  the system, producing precondition traffic, seeding counters. It exists only to make the
  `When` testable.
- **When** — the single action whose side effects the test is trying to observe. There should be
  exactly one. The same `When` action may appear across multiple tests — each test applies it
  to a different `Given` state, or asserts a different property of the outcome.
- **Then** — assertions that tell you what the action changed. A test has one reason to fail:
  the `When` did not produce the expected outcome. That single reason may require multiple
  assertions to fully characterise — asserting several properties of the post-`When` state is
  fine, as long as they all fail for the same underlying reason.

The value of this separation is that it makes the system under test legible from the test
body alone, combined with the test name. A reader should be able to identify the `When`
and immediately know what is being tested without reading the class Javadoc or surrounding
context.

Numbered phase comments (`// Phase 1`, `// Phase 2`) tell you sequence but hide intent:
"Phase 3: reconfigure swaps the filter chain" and "When: proxy reconfigured with new filter
chain" carry the same information, but only the second tells you that reconfigure is the action
being tested, not more setup.