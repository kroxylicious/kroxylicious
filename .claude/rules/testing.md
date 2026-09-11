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

Numbered phase comments (`// Phase 1`, `// Phase 2`) tell you sequence but hide intent —
use `// Given`, `// When`, `// Then` instead.

**Write bare labels only:** `// Given`, `// When`, `// Then` — no text after the label.
The code already says what's happening through types, variable names, and method names.
Text like `// Given: a key pair and passwords` just narrates what the code already says.

## Given — minimum state, no assertions

**Given establishes the side effects the When block depends on — nothing more.** Ask: what
does the When need to act on? Given should establish exactly that. Anything beyond it is
characterising system behaviour, not establishing state.

The confidence a test suite provides comes from two signals: the aggregate (many tests
failing at once tells you something fundamental has snapped) and the individual (one test
failing tells you precisely what is broken). A Given that does more than establish the When's
dependencies corrupts both. It inflates the sea of red — tests that aren't about the broken
thing start failing, making the aggregate harder to read. And it reduces the specificity of a
single failure — you can no longer tell whether the precondition was wrong or the When produced
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

## Awaitility — the condition must fail only via AssertionError

`await().untilAsserted(...)` retries **only** when the condition throws `AssertionError`.
Any other exception escaping the lambda aborts the await and fails the test immediately,
even with time left on the clock. So every operation inside the condition that can fail
transiently must express that failure as an `AssertionError`, not as its own exception.

The most common violation is blocking on a future inside the condition — `Future.get()`,
`CompletableFuture.join()` — which throws `ExecutionException`/`CompletionException` on a
transient failure and defeats the point of the await:

```java
// ❌ Wrong — a transiently failed future aborts the await instead of retrying
await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
    var state = client.fetchState().get(10, TimeUnit.SECONDS);
    assertThat(state).isEqualTo(READY);
});

// ✅ Correct — assert on the future with succeedsWithin and chain the assertion on
// its result; a failed future becomes an AssertionError, which Awaitility retries
await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
    assertThat(client.fetchState())
            .succeedsWithin(10, TimeUnit.SECONDS)
            .satisfies(state -> assertThat(state).isEqualTo(READY));
});
```

AssertJ's `succeedsWithin` accepts `Future` and `CompletableFuture` directly; types that
are neither (e.g. Kafka's `KafkaFuture`) usually offer a conversion such as
`.toCompletionStage().toCompletableFuture()`.

The same principle applies to non-future calls. When a synchronous call inside the
condition throws a specific transient exception, declare it as part of the retry policy
with the type-scoped `.ignoreException(TransientException.class)` on the `await()` chain
(as done with `MeterNotFoundException` in `OperatorMainIT` and `KubernetesClientException`
in `StrimziTestClient`):

```java
await().atMost(Duration.ofSeconds(30))
        .ignoreException(MeterNotFoundException.class)
        .untilAsserted(() -> assertThat(registry.get(meterName).counter().count()).isPositive());
```

Note `ignoreException` suppresses that exception type *anywhere* in the condition. If the
condition does several things and the transient failure must be tolerated at only one
specific sub-call, instead catch the exception at that call site and rethrow it wrapped in
an `AssertionError`. Reach for the blanket, no-arg `.ignoreExceptions()` only as a last
resort — it masks *every* exception, including genuine non-transient failures, turning
what should be an immediate, diagnosable failure into a timeout.
