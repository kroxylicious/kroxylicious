---
paths:
  - "**/*.{java}"
---

# Logging Conventions

## Rationale

We operate on many messages rapidly. Filters may work at record-level, generating huge amounts of log data. We want to avoid silencing errors (renders them invisible) but also avoid log spam.

## Standard Pattern

Use the SLF4J fluent API with key-value pairs for structured logging:

```java
LOGGER.atInfo()
    .addKeyValue("sessionId", sessionId)
    .addKeyValue("apiKey", apiKey)
    .log("processing request");
```

**For hot paths with exceptions**, include conditional stack traces and conditional messages:

```java
LOGGER.atWarn()
    .setCause(LOGGER.isDebugEnabled() ? failureCause : null)
    .addKeyValue("sessionId", sessionId)
    .addKeyValue("error", failureCause.getMessage())
    .log(LOGGER.isDebugEnabled()
        ? "operation failed"
        : "operation failed, increase log level to DEBUG for stacktrace");
```

**Rationale:** When DEBUG is enabled, the stack trace is already logged via `.setCause()`, so the message suffix is redundant and confusing. The conditional ensures the message is contextually appropriate for the logging level.

**Key naming conventions:**
- Use camelCase: `sessionId`, `apiKey`, `correlationId`
- Message describes WHAT happened: `.log("processing request")`
- Key-values provide WHO/WHERE/WHICH: `.addKeyValue("sessionId", ...)`

**Lazy evaluation for expensive operations:**
```java
LOGGER.atDebug()
    .addKeyValue("data", () -> expensiveToString())
    .log("processing data");
```

## Message Text Requirements

**CRITICAL:** Always include descriptive message text in the `.log()` call. Key-value pairs supplement the message but don't replace it.

```java
// ✅ CORRECT - message describes what happened with context from key-values
LOGGER.atWarn()
    .addKeyValue("pluginClass", pluginClass.getName())
    .addKeyValue("name", instanceName)
    .log(pluginClass.getName() + " plugin with name '" + instanceName + "' is deprecated.");

// ❌ WRONG - no message text, only key-values
LOGGER.atWarn()
    .addKeyValue("pluginClass", pluginClass.getName())
    .addKeyValue("name", instanceName)
    .log("plugin is deprecated");  // Too vague, lacks context
```

**Message text must:**
- Describe what happened in sufficient detail
- Be understandable without looking at key-value pairs
- Include critical context (entity names, states, actions)

**Key-value pairs provide:**
- Structured data for filtering/searching
- Additional context (IDs, error details, metrics)
- Machine-readable information

## Logger Naming

**Always use:** `LOGGER` (uppercase) for the logger field name.

```java
private static final Logger LOGGER = LoggerFactory.getLogger(MyClass.class);
```

## Pattern Variations

**Standard pattern** - message in `.log()`:
```java
LOGGER.atWarn()
    .addKeyValue("error", e.getMessage())
    .log("operation failed");
```

**Pattern with arguments** - message template with placeholders:
```java
LOGGER.atWarn()
    .log(LOGGER.isDebugEnabled()
        ? "error handler {}: {}"
        : "error handler {}: {}. Increase log level to DEBUG for stacktrace",
        cause.getClass().getSimpleName(), cause.getMessage());
```

**Pattern with setMessage()** - used in some filters:
```java
LOGGER.atWarn().setMessage(LOGGER.isDebugEnabled()
        ? "Failed to process records, cause: {}."
        : "Failed to process records, cause: {}. Raise log level to DEBUG to see the stack.")
    .addArgument(throwable.getMessage())
    .setCause(LOGGER.isDebugEnabled() ? throwable : null)
    .log();
```

## When to Apply

**Apply structured logging with key-value pairs:**
- All new logging code
- When migrating existing logging code

**Apply conditional stack traces and messages when:**
- Operating in hot paths (per-message, per-record)
- Logging errors that might repeat frequently
- Exception stack traces are large
- Stack traces would generate excessive log volume at WARN level

**Don't apply conditional stack traces when:**
- Startup/initialisation errors (log full stack trace unconditionally)
- Configuration errors (log full detail once)
- Rare errors (unexpected exceptions)
- ERROR level (always include stack trace)

## Message Text Variations

When adding conditional messages for stack traces, **preserve the original message text exactly**. Different files use different wording:

**Most common:** "increase log level to DEBUG for stacktrace"
**Some files:** "Increase log level to DEBUG for stacktrace" (capital I)
**RecordEncryptionFilter:** "Raise log level to DEBUG to see the stack" (different wording)

**Don't normalise these variations** - consistency within each file is more important than consistency across the codebase.

## Log Levels

**ERROR:** Unrecoverable errors requiring operator intervention (always include stack traces)
**WARN:** Recoverable errors, degraded functionality (use conditional stack traces and messages)
**INFO:** Significant events (startup, shutdown, connection events)
**DEBUG:** Detailed diagnostic information including stack traces
**TRACE:** Very detailed (per-message debugging)
