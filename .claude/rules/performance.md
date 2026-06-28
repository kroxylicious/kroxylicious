---
paths:
  - "kroxylicious-filters/**/*.{java}"
  - "kroxylicious-runtime/**/*.{java}"
---

# Performance Rules for Kroxylicious

This is performance-sensitive and highly concurrent code. Follow these rules in hot paths.

## Memory Management

**Avoid unnecessary allocations** in hot paths:
- Filter processing (invoked per-message)
- Codec operations (encoding/decoding)
- Request/response handling

**Pattern:**
```java
// ❌ BAD - allocates every time
return new ArrayList<>(records);

// ✅ GOOD - reuse or stream
return records.stream()...
```

## Netty Buffer Lifecycle

**Critical rules for ByteBuf management:**
- Always call `release()` when finished with a buffer
- Use `retain()` if you need to keep a buffer beyond current scope
- Never access a buffer after calling `release()`

**Pattern:**
```java
ByteBuf buf = ctx.alloc().buffer();
try {
    // use buf
} finally {
    buf.release();
}
```

## ThreadLocal Patterns

Use `ThreadLocal` for codec instances and thread-specific state to avoid allocation overhead:

**Pattern:**
```java
private static final ThreadLocal<ProduceRequestDecoder> DECODER =
    ThreadLocal.withInitial(ProduceRequestDecoder::new);
```

## Async Operations

**Never block event loop threads:**
- Use `CompletionStage` for I/O operations
- Never call `.join()` or `.get()` on futures in filter code
- Use `FilterContext.getFilterDispatchExecutor()` to offload blocking work

**Pattern:**
```java
// ❌ BAD - blocks event loop
Thread.sleep(1000);
var result = future.get();

// ✅ GOOD - async
return CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS)
        .thenCompose(v -> doWork());
```

## Backpressure

**Respect Netty's auto-read mechanism:**
- Don't buffer indefinitely
- Respect channel writability
- Allow backpressure to propagate

## When to Optimise

**Do optimise:**
- Filter processing code paths
- Codec generation and usage
- Response ordering logic
- Protocol message handling

**Don't over-optimise:**
- Configuration parsing (happens once)
- Plugin initialisation (happens once per plugin)
- Logging setup
