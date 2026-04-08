# PROXY Protocol — State Machine Flow

## Pipeline Layout (when proxy protocol is enabled)

```
┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐  ┌──────────────────┐
│ HAProxyMessage   │→ │ HAProxyMessage   │→ │ SniHandler (TLS)│→ │ (handlers added  │
│ Decoder          │  │ Handler          │  │ or plainResolver│  │  by addHandlers) │
│                  │  │                  │  │                 │  │ - RequestDecoder │
│ (auto-removes    │  │ (intercepts      │  │ (resolves       │  │ - ResponseEncoder│
│  after 1 msg)    │  │  HAProxyMessage, │  │  binding, calls │  │ - FrontendHandler│
│                  │  │  passes others)  │  │  addHandlers)   │  │   etc.           │
└──────────────────┘  └──────────────────┘  └─────────────────┘  └──────────────────┘
```

## Key Mechanism: autoRead

`KafkaProxyFrontendHandler.inClientActive()` does:
```java
clientChannel.config().setAutoRead(false);  // stop reading from client
clientChannel.read();                        // trigger exactly ONE read
```

The client is "blocked" until `unblockClient()` is called (which sets `autoRead(true)`).
`unblockClient()` fires when `progressionLatch` reaches 0 (requires both transport
subject built AND state machine reaching Forwarding).

---

## CASE 1: PLAIN (non-TLS) + PROXY Protocol

HAProxy message arrives AFTER `onClientActive`.

```
  TCP connect
      │
      ▼
  ┌─────────────────────────────────────┐
  │ initChannel()                       │
  │  • create KafkaSession              │
  │  • create PCSM(kafkaSession)        │  state = Startup
  │  • add HAProxyDecoder               │
  │  • add HAProxyHandler               │
  │  • add plainResolver                │
  └─────────────────────────────────────┘
      │
      ▼  channelActive propagates to plainResolver
      │
  ┌─────────────────────────────────────┐
  │ plainResolver.channelActive()       │
  │  • resolve binding                  │
  │  • addHandlers() → onBindingRes()   │
  │  • adds FrontendHandler             │
  │  • ctx.fireChannelActive()          │
  └─────────────────────────────────────┘
      │
      ▼
  ┌─────────────────────────────────────┐
  │ FrontendHandler.channelActive()     │
  │  • onClientActive()                 │  state = Startup → ClientActive
  │    • setAutoRead(false)             │  reads blocked
  │    • channel.read()                 │  triggers ONE read
  │    • haProxyMessagePending? NO      │
  └─────────────────────────────────────┘
      │
      ▼  The ONE read picks up PROXY header bytes
      │
  ┌─────────────────────────────────────┐
  │ HAProxyDecoder decodes              │
  │  → emits HAProxyMessage             │
  │  → auto-removes itself              │
  └─────────────────────────────────────┘
      │
      ▼
  ┌─────────────────────────────────────┐
  │ HAProxyHandler.channelRead0()       │
  │  • onHAProxyMessageReceived(msg)    │
  │    • setHAProxyContext()            │  ✓ context saved
  │    • state is ClientActive          │
  │    • state: ClientActive → HAProxy  │  ✓ immediate transition
  │  • ctx.read()                       │  ✓ triggers next read
  └─────────────────────────────────────┘
      │
      ▼  The read picks up ApiVersionsRequest
      │
  ┌─────────────────────────────────────┐
  │ FrontendHandler.channelRead()       │
  │  • onClientRequest(apiVersionsReq)  │
  │    • state: HAProxy → SelectingServer
  │  • connect to backend               │  state → Connecting
  │  • backend active                   │  state → Forwarding
  │  • unblockClient() autoRead(true)   │
  └─────────────────────────────────────┘
      │
      ▼  ✓ Normal operation
```

---

## CASE 2: TLS + PROXY Protocol

HAProxy message arrives BEFORE `onClientActive`.

```
  TCP connect
      │
      ▼
  ┌─────────────────────────────────────┐
  │ initChannel()                       │
  │  • create KafkaSession              │
  │  • create PCSM(kafkaSession)        │  state = Startup
  │  • add HAProxyDecoder               │
  │  • add HAProxyHandler               │
  │  • add SniHandler (TLS)             │
  └─────────────────────────────────────┘
      │
      ▼  Client sends PROXY header (cleartext, before TLS)
      │
  ┌─────────────────────────────────────┐
  │ HAProxyDecoder decodes              │
  │  → emits HAProxyMessage             │
  │  → auto-removes itself              │
  └─────────────────────────────────────┘
      │
      ▼
  ┌─────────────────────────────────────┐
  │ HAProxyHandler.channelRead0()       │
  │  • onHAProxyMessageReceived(msg)    │
  │    • setHAProxyContext()            │  ✓ context saved
  │    • state is Startup (not Active)  │
  │    • haProxyMessagePending = true   │  ✓ deferred
  │  • ctx.read()                       │  (harmless, autoRead still on)
  └─────────────────────────────────────┘
      │
      ▼  Client sends TLS ClientHello
      │
  ┌─────────────────────────────────────┐
  │ SniHandler                          │
  │  • extracts SNI hostname            │
  │  • resolve binding                  │
  │  • addHandlers() → onBindingRes()   │
  │  • adds FrontendHandler             │
  │  • TLS handshake completes          │
  └─────────────────────────────────────┘
      │
      ▼
  ┌─────────────────────────────────────┐
  │ FrontendHandler.channelActive()     │
  │  • onClientActive()                 │  state = Startup → ClientActive
  │    • setAutoRead(false)             │
  │    • channel.read()                 │  triggers ONE read
  │    • haProxyMessagePending? YES     │
  │    • state: ClientActive → HAProxy  │  ✓ replayed transition
  └─────────────────────────────────────┘
      │
      ▼  The read picks up ApiVersionsRequest
      │
  ┌─────────────────────────────────────┐
  │ FrontendHandler.channelRead()       │
  │  • onClientRequest(apiVersionsReq)  │
  │    • state: HAProxy → SelectingServer
  │  • connect to backend               │  state → Connecting
  │  • backend active                   │  state → Forwarding
  │  • unblockClient() autoRead(true)   │
  └─────────────────────────────────────┘
      │
      ▼  ✓ Normal operation
```

---

## State Transition Summary

| Scenario | State transitions |
|----------|-------------------|
| PLAIN    | Startup → ClientActive → HAProxy → SelectingServer → Connecting → Forwarding |
| TLS      | Startup → ClientActive → HAProxy → SelectingServer → Connecting → Forwarding |

Both paths now follow the same state sequence. The difference is only timing:
- PLAIN: HAProxy transition happens immediately in `onHAProxyMessageReceived()`
- TLS: HAProxy transition is deferred and replayed in `onClientActive()`
