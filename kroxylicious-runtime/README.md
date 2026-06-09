# kroxylicious-runtime Module

This module contains the core proxy implementation. See also [`../README.md`](../README.md) for project-wide context.

## Architecture Overview

The runtime implements the Netty-based proxy engine that routes Kafka protocol messages through filter chains whilst maintaining protocol guarantees.

### Key Components

**`KafkaProxyInitializer`**: Sets up the Netty pipeline when a client connects.

**`KafkaProxyFrontendHandler`**: Handles client-side communication (reads requests, writes responses).

**`KafkaProxyBackendHandler`**: Handles broker-side communication (writes requests, reads responses).

**`FilterHandler`**: Adapts filter plugins into the Netty pipeline. One instance per filter in the chain.

**`ResponseOrderer`**: Ensures responses are delivered to clients in request order, even when processed asynchronously.

**`ClientConnectionStateMachine`**: Manages connection state and backpressure propagation between client and broker.

## ResponseOrderer and Kafka Pipelining

**Why it exists:**

Kafka clients use pipelining - they send multiple requests before receiving responses. The Kafka protocol requires responses to be delivered in the same order as requests (FIFO per connection), even though:
- Filters may process messages asynchronously
- Brokers may respond out-of-order
- Multiple requests may be in-flight simultaneously

**How it works:**

- Uses correlation IDs to match requests with responses
- Queues responses that arrive out-of-order
- Drains queued responses when earlier ones arrive
- Handles requests without responses (e.g., some errors) without blocking

**Critical file:** `src/main/java/io/kroxylicious/proxy/internal/ResponseOrderer.java`

## Router Dispatch

When a virtual cluster targets a router, `RouterDispatchHandler` replaces `FilterChainCompletionHandler` at the end of the filter chain. It handles static routes (opaque forwarding by API key) and dynamic routes (deserialised, dispatched to `Router.onRequest`).

**Key classes:**
- `RouterDispatchHandler` — Netty handler; owns the per-connection router lifecycle, response sequencer, and nested router cache
- `RouterContextImpl` — per-request context passed to `Router.onRequest`; implements `sendRequestToNode` for both cluster and router targets
- `NodeIdMapping` — translates between target-cluster node IDs and virtual node IDs (`BijectiveNodeIdMapping` for multi-route, `IdentityNodeIdMapping` for single-route)

### Nested routers

A route can target another router instead of a cluster. When `sendRequestToNode` is called for such a route, `RouterContextImpl.dispatchToNestedRouter` creates a nested `RouterContextImpl` with its own `NodeIdMapping` and invokes the inner router's `onRequest`.

Virtual node IDs from the nested level are translated to the outer level via `outerMapping.toVirtual(outerRoute, nestedVirtual)` before reaching the CCSM's address resolver. This reuses the outer route's slot in the bijective mapping, guaranteeing no collisions. Address caching from METADATA responses uses the translated IDs; response translation uses the nested mapping (so the inner router sees its own virtual space).

Nested `Router` instances are cached per connection in `RouterDispatchHandler` and closed in `handlerRemoved`.

### Per-route filter pipelines

Routes can carry their own filter chain. These filters honour the full Filter API contract — including `sendRequest()`, deferred (async) work, and backpressure — by running in a real Netty pipeline backed by local (in-memory) transport.

**Why local transport?** Platform I/O event loops (epoll, kqueue, io_uring) only support channels with OS file descriptors. `LocalChannel` has no fd, so it cannot be registered on these event loops. A dedicated `DefaultEventLoopGroup` (owned by `KafkaProxy`) provides the event loop for local transport plumbing.

**Threading model.** Filter handlers are added to the local channel's pipeline with `pipeline.addLast(clientEventLoop, handler)`, which causes Netty to dispatch all handler callbacks on the client connection's event loop rather than the local channel's. This preserves the filter threading guarantee: `ctx.executor()` returns the client event loop, and deferred work completes on the correct thread. The `ResponseCaptureHandler` on the peer channel is also bound to `clientEventLoop`.

**Async creation.** `ServerBootstrap` defers bind/connect to the next event loop tick, so pipeline creation returns a `CompletionStage<RouteFilterPipeline>`. The stage is cached per route per connection; only the first request pays the ~1 tick setup cost. Callers must chain with `thenAcceptAsync(callback, clientEventLoop)` because the stage completes on the `DefaultEventLoopGroup` thread.

**Nested routers.** Nested `RouterContextImpl` instances must NOT allocate sequence numbers from the shared `ResponseSequencer`. Nested responses flow back through `dispatchToNestedRouter`'s `thenCompose` chain to the outer context's `submitResponse`, so only the outer context needs a sequence number. A leaked sequence creates a permanent gap that blocks all subsequent responses on the connection. The `ResponseSequencer` includes stall detection that logs a warning after 20 seconds if buffered responses are waiting for a sequence that has not arrived.

Key classes:
- `RouteFilterPipeline` — manages the local channel pair and request/response correlation
- `RouteFilterCompletionHandler` — tail handler; forwards filtered requests and handles `InternalRequestFrame` for `sendRequest()`

### Response handling for routed requests

`PendingResponse` carries a per-response `NodeIdMapping` and `MetadataAddressCacher`. In `onResponse`, addresses are cached **before** `NodeIdResponseTranslator` runs (while node IDs are still target IDs), using the cacher to map to the correct virtual space for the CCSM.

## FilterHandler Async Processing

**Read/Write Future Chains:**

`FilterHandler` maintains two `CompletableFuture` chains:
- **`readFuture`**: Serialises request processing (prevents reading next request until current completes)
- **`writeFuture`**: Serialises response processing (ensures responses are written in order)

**Pattern:**
```java
// Request chain
readFuture = readFuture.thenCompose(v -> processRequest(request));

// Response chain
writeFuture = writeFuture.thenCompose(v -> processResponse(response));
```

This ensures:
- Filters process messages sequentially per connection
- Async filter operations don't break ordering
- Backpressure is correctly propagated

**Double-flush pattern:**

The handler performs two flushes to prevent race conditions:
1. Immediate flush for synchronous writes
2. Flush after async writes complete

**Critical file:** `src/main/java/io/kroxylicious/proxy/internal/FilterHandler.java`

## ClientConnectionStateMachine

**State Transitions:**

Manages connection lifecycle and authentication state:
- `INIT` → `CLIENT_TLS` → `UNAUTHENTICATED` → `AUTHENTICATED` → `CLOSED`
- Transport authentication (mTLS) occurs during `CLIENT_TLS`
- SASL authentication occurs during `UNAUTHENTICATED` → `AUTHENTICATED`

**Backpressure Propagation:**

The state machine tracks both session state and backpressure state:

- **Client applies backpressure** (slow consumer): Proxy pauses broker reads
- **Broker applies backpressure** (TCP buffer full): Proxy pauses client reads
- Filters must respect this - no indefinite buffering

**Pattern:**
```java
// When broker channel becomes unwritable
if (!backendChannel.isWritable()) {
    frontendChannel.config().setAutoRead(false); // Pause client
}

// When broker channel becomes writable again
if (backendChannel.isWritable()) {
    frontendChannel.config().setAutoRead(true); // Resume client
}
```

**Critical file:** `src/main/java/io/kroxylicious/proxy/internal/ClientConnectionStateMachine.java`

## Codec Generation

**When to generate vs reuse:**

- **Generate codec classes**: When adding support for new Kafka API versions or custom message types
- **Reuse existing codecs**: For standard Kafka messages already supported

**Code generation:**

Uses `kroxylicious-krpc-plugin` Maven plugin to generate:
- Decoder classes for each message type
- Encoder classes for each message type
- Filter invoker classes (dispatch to API-specific filter methods)

Generated classes use `ThreadLocal` to avoid allocation overhead in hot paths.

**Pattern:**
```java
// ThreadLocal decoder instance
private static final ThreadLocal<ProduceRequestDecoder> DECODER =
    ThreadLocal.withInitial(ProduceRequestDecoder::new);
```

## Performance Considerations

**Hot path optimisations:**

- **Avoid allocations**: Reuse buffers, use object pools where beneficial
- **Netty buffer management**:
  - Always `release()` buffers you've finished with
  - Use `retain()` if you need to keep a buffer beyond the current scope
  - Never access a buffer after `release()`
- **ThreadLocal usage**: Codecs and state that's thread-specific but reusable
- **CompletionStage composition**: Chain async operations; don't create excessive intermediate futures

**Common performance mistakes:**

❌ Allocating in hot paths:
```java
// Creating new objects per message
return new ArrayList<>(records); // Allocates every time
```

✅ Reuse structures:
```java
// Reuse or stream
return records.stream()...
```

❌ Buffer leaks:
```java
ByteBuf buf = ctx.alloc().buffer();
// Forgot to release!
```

✅ Always release:
```java
ByteBuf buf = ctx.alloc().buffer();
try {
    // use buf
} finally {
    buf.release();
}
```

## Threading Constraints

**Event loop threads:**

- All Netty operations run on event loop threads
- **Never block** - use `CompletionStage` for I/O
- One event loop may handle multiple connections
- Filters may run on different event loops for different connections

**Thread-safe state:**

- Codec instances are `ThreadLocal` (one per thread)
- Connection state is in channels (implicitly thread-safe via event loop)
- Shared state (e.g., metrics, caches) must be thread-safe

## Cross-References

- **Protocol contracts**: See [`../README.md#architecture`](../README.md#architecture)
- **Filter API**: See [`../kroxylicious-api/README.md`](../kroxylicious-api/README.md)
- **Performance**: See [`../.claude/rules/performance.md`](../.claude/rules/performance.md)
