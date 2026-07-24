# Router API — EndpointType sealed interface

This proposal refines the Router API introduced in [Proposal 070](https://github.com/kroxylicious/design/blob/main/proposals/070-routing-api.md)
by replacing the `VirtualNode` marker interface with an `EndpointType` sealed interface
that makes the distinction between bootstrap and broker-specific connections explicit at
the type level.

## Current situation

Since 0.22.0 the Router API exposes two query methods on `RouterContext` that describe
how the current client connection is positioned in the cluster topology:

```java
// "Is the client connected to a specific broker?"
Optional<VirtualNode> virtualNode();

// "Give me a dispatch token so I can send to any broker on a route."
VirtualNode anyNode(String route);
```

`VirtualNode` itself is an opaque marker interface with no accessible state:

```java
public interface VirtualNode {}
```

Together these methods work like this in practice:

```java
// Bootstrap / initial connection — forward to any broker on the default route
if (ctx.virtualNode().isEmpty()) {
    var node = ctx.anyNode("default");
    return ctx.sendRequest(node, header, request)
               .thenCompose(body -> ctx.respondWith(body).completed());
}

// Broker-specific connection — forward to that exact broker
return ctx.sendRequest(ctx.virtualNode().get(), header, request)
           .thenCompose(body -> ctx.respondWith(body).completed());
```

## Problems with the current design

### 1. `Optional` as a type-level distinction is weak

`Optional<VirtualNode>` signals "might be empty", not "is a bootstrap connection".
A router author who sees `Optional` thinks _nullable value_, not _connection phase_.
The semantic — "empty means you connected to a bootstrap address" — is only discoverable
from documentation, not from the type.

### 2. `anyNode()` conflates two distinct operations

`anyNode()` returns a `VirtualNode`, but a bootstrap entry point is not a node.
The caller only uses the return value by passing it directly to `sendRequest()`.
The two-step pattern (`anyNode()` + `sendRequest()`) is really a single route-dispatch
operation that should be expressed atomically:

```java
ctx.sendToRoute("default", header, request);
```

The indirection through `VirtualNode` is a historical artefact: the 0.22.0 design
envisaged `anyNode()` as a node-selection step separate from request dispatch, but no
use case has emerged where a router selects a node and then decides later whether to
actually send to it.

### 3. `VirtualNode` carries no identity

Because `VirtualNode` is an opaque marker, routers cannot reason about which broker
they are targeting at the API level. This is intentional for some cases (the runtime
selects the broker for bootstrap requests) but not for broker-specific connections,
where the broker identity is meaningful and should be surfaced.

## Proposal

### New type: `EndpointType` sealed interface

```java
public sealed interface EndpointType permits Bootstrap, VirtualNode {}
```

Two top-level implementations:

```java
/** The client connected to a bootstrap address (no specific broker). */
public record Bootstrap() implements EndpointType {}

/** The client connected to a broker-specific address. */
public record VirtualNode(int downstreamNodeId) implements EndpointType {}
```

`VirtualNode` is promoted from an opaque marker interface to a value-bearing record.
The `downstreamNodeId` is the virtual (client-facing) node ID assigned by the proxy
— it is not the upstream (target-cluster) node ID.

### Replace `virtualNode()` with `endpoint()`

```java
// Replaces Optional<VirtualNode> virtualNode()
EndpointType endpoint();
```

Router authors use pattern matching:

```java
switch (ctx.endpoint()) {
    case Bootstrap b  -> /* bootstrap path — forward to any broker on a route */
    case VirtualNode vn -> /* broker-specific path — forward to that broker */
}
```

This makes the distinction exhaustive and visible to the compiler.

### Replace `anyNode()` + `sendRequest()` with `sendToRoute()`

```java
// Replaces: var node = ctx.anyNode(route); ctx.sendRequest(node, header, request)
CompletionStage<ApiMessage> sendToRoute(String route, RequestHeaderData header, ApiMessage request);
```

`sendRequest(VirtualNode, ...)` is retained for the broker-specific case, where
the caller already holds a `VirtualNode` from `endpoint()` or `nodeForId(int)`.

## Migration

| 0.22 pattern | 0.23 equivalent |
|---|---|
| `ctx.virtualNode().isEmpty()` | `ctx.endpoint() instanceof Bootstrap` |
| `ctx.virtualNode().get()` | `(VirtualNode) ctx.endpoint()` or pattern variable |
| `ctx.anyNode(route)` + `ctx.sendRequest(node, ...)` | `ctx.sendToRoute(route, ...)` |

### Compatibility

`virtualNode()` and `anyNode()` are deprecated in 0.23 and given default
implementations:

- `virtualNode()` delegates to `endpoint()` — source- and behaviour-compatible.
- `anyNode()` throws `UnsupportedOperationException` with a migration message —
  any existing call sites will fail at runtime; migrate to `sendToRoute()`.

`VirtualNode` changes from an extendable marker interface to a sealed record.
Routers consume `VirtualNode` instances (they do not implement or extend the type),
so this has no practical impact on existing router implementations. Code that
implemented `VirtualNode` directly (unlikely in practice) must migrate.

Both deprecated methods are scheduled for removal in a future minor release.

## Rationale for a sealed type hierarchy

A sealed interface makes the set of valid endpoint kinds closed and
compiler-checkable. Adding a new kind in the future (e.g. `Sidecar`) is a
deliberate, documented API change rather than a silent runtime surprise.
`Optional<VirtualNode>` does not have this property: it can only ever
represent present-or-absent, not a richer taxonomy.

Making `VirtualNode` a record also allows it to carry its `downstreamNodeId`
openly. This is consistent with Proposal 070's description of `VirtualNode`
as an "opaque reference type" — the opacity applies to the upstream (target-cluster)
identity, which is still managed by the runtime. The downstream ID is the proxy's
own virtual ID assignment and is legitimate to expose in the API.