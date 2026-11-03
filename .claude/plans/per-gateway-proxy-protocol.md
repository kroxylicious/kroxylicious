# Plan: Per-Gateway PROXY Protocol Configuration (Design PR #147 Prototype)

## Status

- **Phases 1–3 and Phase 5**: DONE — all runtime source + KafkaClient changes committed on branch
  `prototype/per-gateway-proxy-protocol`. Unit tests pass (4419 tests, 0 failures).
- **Phase 4**: TODO — new `ProxyProtocolIT.java` not yet written; old file still in place.
  Integration tests have not been run yet.
- **Changelog entry**: TODO.

## Context

Design PR #147 moves PROXY protocol configuration from a single global `proxyProtocol` block on
the top-level `Configuration` to a per-`VirtualClusterGateway` setting. This lets different
gateways on the same proxy have different trust levels (e.g. a public LoadBalancer gateway that
_requires_ PROXY headers from the upstream NLB, and an internal ClusterIP gateway that _forbids_
them to prevent spoofing). The `ALLOWED` auto-detect mode is removed (the PROXY spec advises
against it), and `DISABLED` is replaced by `FORBIDDEN` as the safe default.

Scope: proxy runtime only (not the Kubernetes operator).

## Known Prototype Limitation

For TLS connections in `REQUIRED`-without-header and `FORBIDDEN`-with-header cases, rejection
currently fires on the first Kafka `channelRead` after the TLS handshake, rather than before TLS
negotiation as the design specifies. This is an acceptable prototype deviation to note in the PR.

---

## Phase 1 — Config Model Changes

### 1.1 `ProxyProtocolMode.java`
`kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/config/ProxyProtocolMode.java`

- Remove `ALLOWED` and `DISABLED`.
- Add `FORBIDDEN` with `@JsonProperty("forbidden")`.
- Keep `REQUIRED` with `@JsonProperty("required")`.

### 1.2 `ProxyProtocolConfig.java`
`kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/config/ProxyProtocolConfig.java`

- Add a compact constructor that calls `Objects.requireNonNull(mode, "proxyProtocol.mode must be specified")`.
  This makes a bare `proxyProtocol: {}` YAML block fail at startup with a clear message.

### 1.3 `VirtualClusterGateway.java`
`kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/config/VirtualClusterGateway.java`

Add a new `@Nullable @JsonProperty(required = false) ProxyProtocolConfig proxyProtocol` record
component (last parameter). The existing compact constructor already calls
`Objects.requireNonNull` for other fields; `proxyProtocol` being `null` is valid and means
`FORBIDDEN`.

Add helper:
```java
public ProxyProtocolMode proxyProtocolMode() {
    return proxyProtocol != null ? proxyProtocol.mode() : ProxyProtocolMode.FORBIDDEN;
}
```

Update `toString()` to include the new field.

**Impact**: all direct `new VirtualClusterGateway(...)` call sites need a trailing `null` argument.
Test-code that uses the generated `VirtualClusterGatewayBuilder` picks up a new `withProxyProtocol()`
method for free once the annotation processor re-runs.

### 1.4 `Configuration.java`
`kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/config/Configuration.java`

- Remove the `@Nullable ProxyProtocolConfig proxyProtocol` field.
- Remove the `proxyProtocolMode()` helper method.
- To produce a clear startup error when the old global `proxyProtocol:` key is present in YAML,
  add a setter annotated `@JsonProperty("proxyProtocol")` that throws
  `IllegalConfigurationException("Global proxyProtocol is no longer supported. Move it inside each gateway.")`.

---

## Phase 2 — Runtime / Pipeline Changes

### 2.1 `EndpointGateway.java` (interface)
`kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/net/EndpointGateway.java`

Add:
```java
/** The PROXY protocol enforcement mode for this gateway. */
ProxyProtocolMode proxyProtocolMode();
```

### 2.2 `VirtualClusterModel` inner class `VirtualClusterGatewayModel`
`kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/model/VirtualClusterModel.java`

Implement the new interface method by delegating to the config record:
```java
@Override
public ProxyProtocolMode proxyProtocolMode() {
    return gateway.proxyProtocolMode(); // gateway is the VirtualClusterGateway config record
}
```

### 2.3 `HaProxyProtocolDetectionHandler.java`
`kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/HaProxyProtocolDetectionHandler.java`

- Remove the `ProxyProtocolMode mode` field and constructor parameter.
- Simplify `INVALID` case: always pass through (remove the `REQUIRED` close branch). Enforcement
  is now decoupled from detection and happens downstream.
- Update Javadoc to state this handler is a **pure detector**: it sniffs the connection, installs
  `HAProxyMessageDecoder` + `HaProxyMessageHandler` when a PROXY header is found, and passes bytes
  through unconditionally otherwise.

### 2.4 `KafkaProxyInitializer.java`
`kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/KafkaProxyInitializer.java`

**Remove** the `ProxyProtocolMode proxyProtocolMode` field and constructor parameter. Update the
`@SuppressWarnings("java:S107")` count accordingly.

**`initChannel()`**: Remove the `!= DISABLED` guard. Always add `HaProxyProtocolDetectionHandler`:
```java
ch.pipeline().addLast("HaProxyProtocolDetectionHandler",
        new HaProxyProtocolDetectionHandler(kafkaSession));
```

**`addHandlers()`**: Add enforcement as the very first step (right after
`pipeline.remove(LOGGING_INBOUND_ERROR_HANDLER_NAME)`):
```java
var gatewayMode = binding.endpointGateway().proxyProtocolMode();
pipeline.addLast("proxyProtocolEnforcer",
        new ProxyProtocolEnforcementHandler(gatewayMode, kafkaSession));
```

Pipeline order after the change (plain channel, before first data):
```
[HaProxyProtocolDetectionHandler] → [proxyProtocolEnforcer] → [networkLogger?] → [requestDecoder] → ...
```
For a TLS channel (detection handler removed itself before `addHandlers()` is called):
```
[SniHandler] → [proxyProtocolEnforcer] → [networkLogger?] → [requestDecoder] → ...
```

**New inner class `ProxyProtocolEnforcementHandler`** (or a top-level package-private class —
prefer top-level for testability):

```java
class ProxyProtocolEnforcementHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = ...;
    private final ProxyProtocolMode mode;
    private final KafkaSession session;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        boolean hadProxy = session.haProxyContext() != null;
        if (mode == REQUIRED && !hadProxy) {
            LOGGER.atWarn().addKeyValue("channel", ctx.channel())
                    .log("Connection rejected — PROXY protocol header required but not received...");
            ReferenceCountUtil.release(msg);
            ctx.close().addListener(logFailure(LOGGER, "close after missing PROXY header"));
        } else if (mode == FORBIDDEN && hadProxy) {
            LOGGER.atWarn().addKeyValue("channel", ctx.channel())
                    .log("Connection rejected — PROXY protocol header received but forbidden...");
            ReferenceCountUtil.release(msg);
            ctx.close().addListener(logFailure(LOGGER, "close after forbidden PROXY header"));
        } else {
            ctx.pipeline().remove(this);
            ctx.fireChannelRead(msg);
        }
    }
}
```

`session.haProxyContext()` already exists (returns the stored `HaProxyContext` or `null`).

### 2.5 `KafkaProxy.java`
`kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/KafkaProxy.java`

- Remove `var proxyProtocolMode = config.proxyProtocolMode();`.
- Remove the `proxyProtocolMode` argument from both `new KafkaProxyInitializer(...)` calls.

---

## Phase 3 — `KafkaClient` Extension

`kroxylicious-integration-test-support/src/main/java/io/kroxylicious/testing/integration/client/KafkaClient.java`

Add a `@Nullable HAProxyMessage proxyMessage` field. Add two static factories:

```java
public static KafkaClient withProxyProtocol(String host, int port, HAProxyMessage message) {
    return new KafkaClient(host, port, null, message);
}

public static KafkaClient withProxyProtocol(String host, int port, HAProxyMessage message,
                                             SslContext sslContext) {
    return new KafkaClient(host, port, sslContext, message);
}
```

Refactor the existing `public KafkaClient(String host, int port, SslContext clientSslContext)`
constructor to call a private four-arg constructor:
```java
private KafkaClient(String host, int port, @Nullable SslContext sslContext,
                    @Nullable HAProxyMessage proxyMessage)
```

**`ensureChannel()` changes** — two behaviours based on `proxyMessage`:

*Without PROXY (existing path, unchanged except extracted)*:
```
if (sslContext != null) p.addLast(sslContext.newHandler(...));
p.addLast(encoder, decoder, handler);
// connect listener: candidate.complete(channel)
```

*With PROXY preamble*:
```
p.addLast(HAProxyMessageEncoder.INSTANCE);
p.addLast(new ChannelInboundHandlerAdapter() {        // one-shot preamble sender
    @Override public void channelActive(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(proxyMessage).addListener(writeFuture -> {
            ctx.pipeline().remove(HAProxyMessageEncoder.class);
            ctx.pipeline().remove(this);
            if (sslContext != null) {
                var sslHandler = sslContext.newHandler(ctx.alloc(), host, port);
                ctx.pipeline().addFirst(sslHandler);
                sslHandler.handshakeFuture().addListener(hf -> {
                    candidate.complete(ctx.channel()); ctx.fireChannelActive();
                });
            } else {
                candidate.complete(ctx.channel()); ctx.fireChannelActive();
            }
        });
    }
});
p.addLast(encoder, decoder, handler);
// no connect listener — candidate.complete() called above
```

The PROXY + TLS pipeline sends the preamble as plain bytes, then adds the SSL handler so the
TLS ClientHello follows, matching the wire order the proxy expects.

---

## Phase 4 — Integration Tests

### 4.1 Delete `ProxyProtocolIT.java`
`kroxylicious-integration-tests/src/test/java/io/kroxylicious/it/ProxyProtocolIT.java`

### 4.2 Write new `ProxyProtocolIT.java`

Config helper: add `withProxyProtocol(ProxyProtocolConfig pp)` to
`VirtualClusterGatewayFluent/Builder` (generated automatically). Update
`KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder` usages in tests accordingly.
The test class builds configs using the gateway-level fluent builder.

Plain-channel test cases (use `mockKafkaKroxyliciousTester`):

| Test method | Gateway mode | Client | Expected |
|---|---|---|---|
| `requiredMode_plainChannel_withProxyHeader_succeeds` | required | `KafkaClient.withProxyProtocol(...)` | success (ApiVersions response) |
| `requiredMode_plainChannel_withoutProxyHeader_rejects` | required | plain `KafkaClient` | `failsWithin(5s)` |
| `forbiddenMode_plainChannel_withoutProxyHeader_succeeds` | forbidden | plain `KafkaClient` | success |
| `forbiddenMode_plainChannel_withProxyHeader_rejects` | forbidden | `KafkaClient.withProxyProtocol(...)` | `failsWithin(5s)` |

TLS-channel test cases (same four, add TLS via `defaultPortIdentifiesNodeGatewayBuilder` + keystore):

| Test method | Note |
|---|---|
| `requiredMode_tlsChannel_withProxyHeader_succeeds` | `KafkaClient.withProxyProtocol(host, port, msg, TRUST_ALL_CLIENT_SSL_CONTEXT)` |
| `requiredMode_tlsChannel_withoutProxyHeader_rejects` | plain TLS client → `failsWithin(5s)` |
| `forbiddenMode_tlsChannel_withoutProxyHeader_succeeds` | plain TLS client → success |
| `forbiddenMode_tlsChannel_withProxyHeader_rejects` | `KafkaClient.withProxyProtocol(...)` → `failsWithin(5s)` |

Config-validation test cases (no tester needed, use `ConfigParser.parseConfiguration` directly):

- `proxyProtocolWithoutMode_failsAtStartup` — YAML with `proxyProtocol: {}` on a gateway → exception
- `globalProxyProtocolField_failsAtStartup` — YAML with top-level `proxyProtocol:` → exception

---

## Phase 5 — Unit Test Updates

- **`HaProxyProtocolDetectionHandlerTest`** — remove all `mode`-parameter variants; add a test
  that `INVALID` always passes bytes through.
- **`ConfigurationValidationTest`** — remove tests for old global modes; add per-gateway cases.
- **`ConfigParserTest`** — update YAML fixtures that used top-level `proxyProtocol:`.
- **`KafkaProxyInitializerTest`** — remove mode param from construction; add `ProxyProtocolEnforcementHandler`
  unit tests (REQUIRED-no-proxy closes, FORBIDDEN-with-proxy closes, happy paths pass through).
- **`HaProxyMessageHandlerTest`** — no change expected.

---

## Verification

```bash
# Unit tests for changed modules
mvn -pl kroxylicious-runtime,kroxylicious-integration-test-support test \
    -Derrorprone.skip=true -P'!qa'

# Integration tests (proxy protocol suite only, fast)
mvn -pl kroxylicious-integration-tests verify -Dit.test=ProxyProtocolIT \
    -Derrorprone.skip=true -P'!qa'

# Broader runtime IT sanity check
mvn -pl kroxylicious-integration-tests verify \
    -Derrorprone.skip=true -P'!qa'
```

---

## Files Changed Summary

| File | Change |
|---|---|
| `…/config/ProxyProtocolMode.java` | Remove ALLOWED/DISABLED; add FORBIDDEN |
| `…/config/ProxyProtocolConfig.java` | Null-check mode in compact ctor |
| `…/config/VirtualClusterGateway.java` | Add proxyProtocol component + helper |
| `…/config/Configuration.java` | Remove global field; add error setter |
| `…/internal/net/EndpointGateway.java` | Add proxyProtocolMode() to interface |
| `…/model/VirtualClusterModel.java` | Implement proxyProtocolMode() in inner class |
| `…/internal/HaProxyProtocolDetectionHandler.java` | Remove mode; simplify INVALID branch |
| `…/internal/ProxyProtocolEnforcementHandler.java` | **New file** — per-gateway enforcement |
| `…/internal/KafkaProxyInitializer.java` | Remove mode field; always add detector; add enforcer in addHandlers() |
| `…/KafkaProxy.java` | Remove proxyProtocolMode wiring |
| `…/testing/integration/client/KafkaClient.java` | Add proxyMessage field + factories |
| `…/it/ProxyProtocolIT.java` | Replace with new per-gateway tests |
| Various unit test files | Updates per above |
