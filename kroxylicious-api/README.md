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

## Router API

### VirtualNode

`VirtualNode` is an opaque reference type for node identity in the Router API. Routers obtain instances from `RouterContext` methods and pass them to `sendRequest()`. The type is intentionally opaque — routers must not inspect or construct instances directly.

**Why opaque?** The integer-based port-per-broker networking model encodes route and target broker into a single int. An alternative networking model (where proxy instances act as brokers) needs richer routing information. `VirtualNode` hides this difference so routers work with either model unchanged.

**Key methods on `RouterContext`:**
- `virtualNode()` — the node the client connected to (empty for bootstrap)
- `anyNode(route)` — an arbitrary node on a route (for discovery requests)
- `nodeForId(int)` — converts a protocol integer (from METADATA, FIND_COORDINATOR responses) to a `VirtualNode`. This is the bridge between the Kafka wire protocol (integers) and the opaque API. Routers need this when interpreting node IDs in protocol response bodies — for example, when merging METADATA responses from multiple routes.
- `sendRequest(node, header, request)` — sends to a specific node

### TopologyService

`TopologyService` is an opt-in topology cache for routers that need leader, coordinator, broker, or topic ID information. Routers obtain it from `RouterFactoryContext.topologyService()` during `RouterFactory.initialize()`. Routers that never call `topologyService()` pay no cost — no cache is created.

**Cache population model:** The cache is populated as a **side effect** of METADATA responses flowing through the routing pipeline. When any request sent via `RouterContext.sendRequest()` produces a METADATA response, the runtime updates the topology cache before completing the router's `CompletionStage`. This means after a METADATA request's future completes, the cache is guaranteed to reflect that response.

**Read-only queries (synchronous):**
- `leaderOf(topicName, partitionIndex)` — cached partition leader
- `coordinatorOf(route, keyType, key)` — cached coordinator
- `partitionInfoFor(topicName, partitionIndex)` — leader + replicas + ISR (for follower-fetch)
- `brokerInfo(node)` — host, port, rack (for AZ-aware routing)

**Active methods (may send requests):**
- `topicNames(Set<Uuid>)` — batched topic ID resolution
- `ensureLeadersCached(Map<String, Set<String>>)` — batched leader cache warming
- `discoverCoordinator(route, keyType, key)` — two-hop coordinator discovery (METADATA then FIND_COORDINATOR)

**Invalidation:** `invalidateRoute(route)` performs coarse invalidation — clears all partition info, coordinators, and broker info for a route. Topic ID→name mappings are not cleared (they are stable within a cluster). The router calls this when it observes staleness indicators (e.g. `NOT_LEADER_OR_FOLLOWER`) in responses. No background refresh is fired — the client drives the refresh via its own METADATA request.

**Why coarse invalidation?** A single `invalidateRoute()` replaces what would otherwise be `invalidateLeader()`, `invalidateCoordinator()`, `invalidateNode()`. Over-invalidation is acceptable because the cache is repopulated cheaply from the client's next METADATA request. This avoids an ever-growing set of invalidation methods as more cached entity types are added.

**Why the router handles invalidation, not the runtime:** The runtime would need to deserialise an open-ended and growing set of response types to scan for error codes. Different error codes have different semantics. Routers that don't use the topology cache shouldn't pay the deserialisation cost.

## Cross-References

- **Protocol contracts**: See [`../README.md#architecture`](../README.md#architecture)
- **Security model**: See [`../README.md#security-model`](../README.md#security-model)
- **Testing filters**: See `kroxylicious-filter-test-support/` module
- **Example filters**: See `kroxylicious-filters/` for production filter implementations
