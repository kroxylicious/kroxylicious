---
paths:
  - "**/*.{java}"
---

# Logging Conventions

## Rationale

The proxy operates on many messages rapidly. 
Filters may work at record-level, generating huge amounts of log data. 
We want to avoid silencing errors (renders them invisible) but also avoid log spam.

## Logger field naming

**Always use:** `LOGGER` (uppercase) for the logger field name.

```java
private static final Logger LOGGER = LoggerFactory.getLogger(MyClass.class);
```

## Log Levels

**ERROR:** Unrecoverable errors requiring operator intervention (always include stack traces)
**WARN:** Recoverable errors, degraded functionality (use conditional stack traces and messages)
**INFO:** Significant events (startup, shutdown, connection events)
**DEBUG:** Detailed diagnostic information including stack traces
**TRACE:** Very detailed (per-message debugging)

## Logger API usage

Some things you should actively seek to do:

* Do use structured logged (`addKeyValue()`) to capture relevant context from the call site.
* Do try to use keys consistently. 
* Do Lazy evaluation for expensive operations:
    ```java
    LOGGER.atDebug()
        .addKeyValue("data", () -> expensiveToString())
        .log("processing data");
    ```
* Do put the message in the terminal `log()` call, rather than `setMessage()`.

Some things to avoid:

* Do **not** use message interpolation (`addArgument()`, or the non-unary verions of `log()`) to capture relevant context from the call site. 
  Use `addKeyValue()` instead.
* Do **not** use Mapped Diagnostic Contexts. 
  They store the context on the thread.
  Our use of Netty and other asynchronous techniques means that it's difficult to manage that state correctly.
* Do **not** add an exception cause (`setCause()`) unconditionally when on a hot path (e.g. per-request, per-response, per-record), or when 
  stack traces would generate excessive log volume at WARN level.
  Instead apply the following pattern:
    ```java
    LOGGER.atWarn()
    .setCause(LOGGER.isDebugEnabled() ? failureCause : null)
    .addKeyValue("error", failureCause.getMessage())
    .log(LOGGER.isDebugEnabled()
        ? "operation failed"
        : "operation failed, increase log level to DEBUG for stacktrace");
    ```

**Don't apply conditional stack traces when:**
- Startup/initialisation errors (log full stack trace unconditionally)
- Configuration errors (log full detail once)
- Rare errors (unexpected exceptions)
- ERROR level (always include stack trace)

## Key naming conventions

- Use camelCase: `sessionId`, `apiKey`, `correlationId`
- Message describes WHAT happened: `.log("processing request")`
- Key-values provide WHO/WHERE/WHICH: `.addKeyValue("sessionId", ...)`

When deciding what to record as structured context:

* Always include the `sessionId` if it's known at that point in the code (it's main purpose is for correlating events)
* Always include the `filter` if the event is specific to a filter.
* Always include the `virtualCluster` if the event is specific to a virtual cluster.

When choosing the key to use with `addKeyValue()`, try to be consistent in capturing the context:

* The proxy session: `sessionId`
* The name of a filter plugin: `filter` (not `filterName`)
* The name of a virtual cluster: `virtualCluster`
* An authenticated subject: `subject` (don't just log one of the `Principals`).
* An HTTP status code: `statusCode`
* An URL (for example in an HTTP request): `requestUrl`
* Prefer using an `address` rather than a separate keys for `host` and `port`.
* A Kafka API key from a request or response: `apiKey`
* A Kafka API version from a request or response: `apiVersion`
* An error code from a Kafka request or response: `errorCode`
* An error message from an exception or from a Kafka request or response: `error`
* A Kafka topic name: `topicName` (not just `name`)
* A Kafka topic id: `topicId`
* A Kafka topic parition index: `partition`
* An a Kafka request or response correlationId: Either `serverCorrelationId` or `clientCorrelationId` (not just `correlationId`)

In `kroxylicious-operator` specifically:

* A kubernetes resource name: `name`
* A kubernetes resource namespace: `namespace`
* A kubernetes resource kind: `kind`

## Things not to log

The following list applies to all logging except at `TRACE` level:

* Do not log objects containing sensitive information, including things like secret keys, passwords, data passed in a SASL exchange, and so on.
  Be aware that `toString()` will often invoke `toString()` of fields of an object, so logging a wrapper object can still leak sensitive data.
* You must assume that all Kafka records and Kafka protocol messages contain sensitive data. 
  Therefore they should not be logged.
  Instead, extract fields known not to be sensitive and log those. 
* When declaring classes or records proactively override `toString()` to redact sensitive data. When you do so, annotate the class with `@SanitizedToString`.

