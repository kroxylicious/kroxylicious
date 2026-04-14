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

**`ProxyChannelStateMachine`**: Manages connection state and backpressure propagation between client and broker.

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

## ProxyChannelStateMachine

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

**Critical file:** `src/main/java/io/kroxylicious/proxy/internal/ProxyChannelStateMachine.java`

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
