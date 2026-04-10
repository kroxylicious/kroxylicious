# kroxylicious-api Module

This module defines the public Filter API and other plugin interfaces. See also [`../README.md`](../README.md) for project-wide context.

## API Roles

**Filter Implementers**: Developers writing filter plugins that intercept and process Kafka messages.

**Runtime (Proxy)**: The `kroxylicious-runtime` module that invokes filters and provides infrastructure via `FilterContext`.

---

# For Filter Implementers

This section describes how to implement filters and the constraints you must follow.

## Filter Types and Selection

**When to use each filter type:**

- **`RequestFilter`**: Intercepts all request types. Use when you need to handle multiple request APIs or when the specific API isn't known at compile time.
- **`ResponseFilter`**: Intercepts all response types. Use when you need to handle multiple response APIs.
- **API-specific filters** (e.g., `ProduceRequestFilter`, `FetchResponseFilter`): Type-safe access to specific message types. Prefer these when you know which APIs you're handling at compile time.

**Filter interfaces are generated** from Kafka message specifications. See `kroxylicious-krpc-plugin` for the code generator.

## Implementation Constraints

**Threading:**

- Filter methods are called on Netty event loop threads - **you must never block**
- Multiple filter instances may exist per connection (one per filter in the chain)
- Filters on different connections run independently (possibly on different event loops)
- **Requirement**: Use `CompletionStage` for any I/O or slow operations
- **State management**: If your filter maintains state, it must be thread-safe or connection-scoped

**Lifecycle:**

- `FilterFactory.createFilter(FilterFactoryContext, PluginConfig)` is called once per filter instance
- Your filter methods are invoked for each matching message
- Return `CompletionStage<FilterResult>` - the runtime waits for completion before proceeding
- **No explicit cleanup**: Don't maintain resources requiring cleanup in filters; manage them at the factory level

## Returning FilterResults

**Requirement**: All filter methods must return `CompletionStage<FilterResult>`.

Your result determines what the runtime does next:

**`forward(message)`**: Pass the message (possibly modified) to the next filter or the broker/client
```java
return context.forwardRequest(modifiedRequest);
```

**`drop()`**: Discard the message. The runtime triggers a channel read to keep the connection active.
```java
return context.requestFilterResultBuilder().drop().completed();
```

**`shortCircuitResponse(response)`**: Generate a response locally without contacting the broker. Only valid for requests that have responses.
```java
return context.requestFilterResultBuilder()
       .shortCircuitResponse(new ProduceResponseData()...)
       .completed();
```

**`closeConnection()`**: Terminate the connection after optional flush. Use sparingly (usually for protocol violations).
```java
return context.requestFilterResultBuilder()
       .withCloseConnection()
       .drop()
       .completed();
```

**Common mistake**: Forgetting to call `.completed()` on the builder.

## Asynchronous Processing

**Requirement**: You must return `CompletionStage<FilterResult>` to support async operations without blocking.

**Pattern for external service calls:**
```java
@Override
public CompletionStage<RequestFilterResult> onProduceRequest(
        ProduceRequestData request,
        FilterContext context) {
    return externalService.validateAsync(request)
            .thenCompose(valid -> {
                if (valid) {
                    return context.forwardRequest(request);
                } else {
                    return context.requestFilterResultBuilder()
                            .shortCircuitResponse(errorResponse())
                            .completed();
                }
            });
}
```

**Critical constraint**: Never block the event loop:
```java
// ❌ BAD - blocks event loop
Thread.sleep(1000);

// ✅ GOOD - async delay
CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS)
        .execute(() -> {...});
```

## Using Subject and Principals

**Subject composition rules you must follow:**
- Non-anonymous subjects must have exactly one `User` principal
- Use `@Unique` annotation on your custom principal types to enforce at-most-one constraint

**Pattern for extracting principals:**
```java
Subject subject = context.getAuthenticatedSubject();
Optional<User> user = subject.principals(User.class).findFirst();
if (user.isPresent()) {
    String username = user.get().name();
    // ...
}
```

**Creating custom principals:**
```java
// Implement Principal interface
public record MyPrincipal(String value) implements Principal {
    @Override
    public String type() {
        return MyPrincipal.class.getName();
    }

    @Override
    public String name() {
        return value;
    }
}
```

---

# What the Runtime Provides

This section describes the infrastructure and guarantees the proxy runtime provides to your filter via `FilterContext`.

## FilterContext API

The runtime provides these services via `FilterContext`:

**Message forwarding**:
- `forwardRequest(request)` - Forward request to next filter or broker
- `forwardResponse(response)` - Forward response to next filter or client

**Result builders**:
- `requestFilterResultBuilder()` - Build request filter results
- `responseFilterResultBuilder()` - Build response filter results

**Async execution**:
- `getFilterDispatchExecutor()` - Executor for offloading work from event loop

**Connection metadata**:
- Client address, session ID, connection information

**Authentication state**:
- `getAuthenticatedSubject()` - Authenticated client identity (may be anonymous)

## Runtime Guarantees

**Message ordering**: The runtime ensures FIFO response ordering per connection, even when your filter processes messages asynchronously. See `ResponseOrderer` in `kroxylicious-runtime`.

**Backpressure**: The runtime propagates backpressure between client and broker. Your filter must not buffer indefinitely.

**Protocol compliance**: The runtime validates that your filter results don't violate Kafka protocol contracts (e.g., can't short-circuit a request that has no response type).

**Thread safety**: The runtime ensures filter methods are called in a thread-safe manner (via event loop serialization), but your filter's internal state must be thread-safe if shared across connections.

---

# Implementation Guidance

## Common Implementation Mistakes

**❌ Blocking operations on event loop:**
```java
// Never do this in a filter
database.query("SELECT ...").get(); // blocks!
```

**✅ Use async APIs:**
```java
return database.queryAsync("SELECT ...")
        .thenCompose(result -> context.forwardRequest(request));
```

**❌ Assuming synchronous processing:**
```java
// Filter assumes next filter runs synchronously
stateMap.put(correlationId, state);
return context.forwardRequest(request);
// Next filter may not run yet!
```

**✅ Handle state in async completion:**
```java
return context.forwardRequest(request)
        .thenApply(result -> {
            stateMap.put(correlationId, state);
            return result;
        });
```

**❌ Violating protocol contracts:**
```java
// Modifying message structure in ways that break schema
record.value(new byte[0]); // May break compression
```

**✅ Preserve protocol semantics:**
```java
// Transform value but preserve structure
byte[] newValue = transform(record.value());
record.value(newValue);
```

**❌ Not handling all result cases:**
```java
// Incomplete - what if response is null?
return context.forwardResponse(response);
```

**✅ Handle all cases:**
```java
if (response == null) {
    return context.responseFilterResultBuilder().drop().completed();
}
return context.forwardResponse(response);
```

## Cross-References

- **Protocol contracts**: See [`../README.md#architecture`](../README.md#architecture)
- **Security model**: See [`../README.md#security-model`](../README.md#security-model)
- **Testing filters**: See `kroxylicious-filter-test-support/` module
- **Example filters**: See `kroxylicious-filters/` for production filter implementations
