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

## Given — minimum state, no assertions

**Given provides only the minimum state needed to make When testable — nothing more.**
The confidence a test suite provides comes from two signals: the aggregate (many tests
failing at once tells you something fundamental has snapped) and the individual (one test
failing tells you precisely what is broken). A Given that does more than minimum setup
corrupts both. It inflates the sea of red — tests that aren't about the broken thing start
failing, making the aggregate harder to read. And it reduces the specificity of a single
failure — you can no longer tell whether the precondition was wrong or the When produced
the wrong outcome.

**Given must not contain assertions.** An assertion in Given gives the test two reasons to
fail: the precondition is wrong, or the When produced the wrong outcome. When the test
fails you can't tell which. If a precondition is genuinely worth asserting, extract it to a
dedicated test that owns that concern — then every test that depends on it can simply trust it.

If you need to guard against a test running in an environment that can't support it,
`assumeThat` is appropriate — for example, checking that `kubectl` is on the PATH or that
EPOLL or io_uring can be enabled. These are deployment facts, not behavioral contracts. For
behavioral preconditions (did my filter initialize? does this cluster exist?), `assumeThat`
is the wrong tool — those imply a testable contract that either already has coverage elsewhere
in the suite, or needs a dedicated test added. The right answer is never to silently skip.
