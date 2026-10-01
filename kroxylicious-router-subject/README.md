# Subject Router

Routes each client connection to a downstream cluster based on the connection's authenticated identity.
Implements [design proposal #140](https://github.com/kroxylicious/design/pull/140).

This is a preview feature, gated behind `KROXYLICIOUS_UNLOCK_ROUTING=true` (see [Preview gate](#preview-gate)).

## How it routes

The router asks a pluggable `RouteSelector` to map the connection's authenticated `Subject` to a route name, then pins the connection to that route:

1. **Authenticated request** — the selector picks a route for the subject. The first pick pins the connection; a later request that resolves to a *different* route closes the connection rather than switching clusters.
2. **Anonymous `API_VERSIONS`** — before a client authenticates (mTLS handshake pending, or pre-SASL), the router fans the request out to every configured route and returns the intersection of supported API versions, so the client negotiates a version range every downstream cluster actually supports. See [Anonymous `API_VERSIONS` fan-out](#anonymous-api_versions-fan-out).
3. **Anonymous `SASL_HANDSHAKE`/`SASL_AUTHENTICATE`** — under SASL *passthrough inspection* (e.g. `kroxylicious-sasl-inspection`), the proxy observes but does not terminate the exchange, so it must still reach a real broker anonymously. See [SASL passthrough inspection](#sasl-passthrough-inspection).
4. **Everything else anonymous** — rejected fail-closed.
5. **No route for the subject** (selector returns nothing, or throws) — rejected fail-closed.

If the connection is to a broker-specific gateway endpoint (`RouterContext#virtualNode()` non-empty), the router sends there rather than to an arbitrary node of the chosen route, so a client that dialed a specific broker keeps talking to it.

Fail-closed rejections use `Errors.SASL_AUTHENTICATION_FAILED`; the runtime maps it to a per-API error response and the connection is closed.

## Configuration

```yaml
routerDefinitions:
  - name: subject-router
    type: SubjectRouter
    config:
      selector: UserNameMatch
      selectorConfig:
        mappings:
          - route: team-a
            principals: [ "CN=alice", "CN=bob" ]
          - route: team-b
            principals: [ "CN=carol" ]
        defaultRoute: team-a   # optional
    routes:
      - name: team-a
        id: 0
        target:
          cluster: cluster-a
      - name: team-b
        id: 1
        target:
          cluster: cluster-b
```

| Field | Type | Description |
|---|---|---|
| `selector` | string | The `RouteSelector` implementation name (plugin simple name or FQCN). |
| `selectorConfig` | object | The selector's own configuration, shaped by the chosen `selector`. |

### `RouteSelector` SPI

`RouteSelector` is a nested plugin point (like `KekSelectorService` in `kroxylicious-record-encryption`): implementations carry their own `@Plugin(configType = ...)` and are discovered via `META-INF/services`. An implementation:

- Is constructed with a no-arg constructor, then `initialize(config)` is called once before any routing decision — config never flows through the constructor.
- Implements `selectRoute(Subject, RouteSelectorContext)`, returning the route name or empty to reject.
- Optionally implements `referencedRoutes()` to declare, ahead of time, every route name it may return. The router validates these against the router's declared routes at startup and fails fast on an unknown route. A selector that routes dynamically (can't enumerate routes upfront) returns `Optional.empty()` from `referencedRoutes()` to opt out of this check.

### Built-in selector: `UserNameMatch`

Maps principal names to routes.

```yaml
selectorConfig:
  mappings:
    - route: team-a
      principals: [ "CN=alice", "CN=bob" ]
    - route: team-b
      principals: [ "CN=carol" ]
  defaultRoute: team-a   # optional: used for any principal not listed above
```

| Field | Type | Description |
|---|---|---|
| `mappings` | list | Each entry maps one route to the principal names sent there. |
| `defaultRoute` | string, optional | Route used for a principal that matches no mapping. Omit to reject unmapped principals fail-closed. |

Startup fails if the same principal name appears in two different mappings.

The principal name matched is `Subject.uniquePrincipalOfType(User.class).map(User::name)` — for mTLS this is the client certificate's subject DN as rendered by `X500Principal.getName(X500Principal.RFC1779, ...)` (with `emailAddress` mapped from its OID); for SASL termination it is the authenticated username.

## Anonymous `API_VERSIONS` fan-out

`SubjectRouter` never declares static routes (`staticRoutes()` is always empty), because which downstream cluster a connection belongs to depends on identity that is not known until routing time — unlike a router that always sends a given API key to the same fixed place. This means `API_VERSIONS` is dynamically routed like everything else, and the router itself must handle it specially for the pre-authentication case: the runtime does not compute a cross-route version intersection on a router's behalf.

For an anonymous connection's `API_VERSIONS` request, the router sends the request to every declared route concurrently and combines the responses:

- An API key survives only if every route's response includes it, with the version range narrowed to the overlap (`minVersion = max(...)`, `maxVersion = min(...)`). An API key present in only some routes' responses is dropped.
- If any route's response carries a non-zero top-level `errorCode`, the whole request is rejected fail-closed rather than returning a partial intersection.
- Feature blocks (`supportedFeatures`, `finalizedFeatures`, `finalizedFeaturesEpoch`, `zkMigrationReady`) are not intersected — the first route's response is copied as-is. This is a simplification; routes with materially different feature sets are not supported by this fan-out.
- The connection is not pinned to any route during fan-out — pinning happens on the first *authenticated* request.

## SASL passthrough inspection

Some SASL filters (`kroxylicious-sasl-inspection`) do *passthrough inspection*: they observe `SASL_HANDSHAKE`/`SASL_AUTHENTICATE` on the wire to learn the client's identity, but forward the exchange to a real broker to actually validate the credentials — unlike `kroxylicious-sasl-termination`, which validates at the proxy and never forwards the raw exchange. Under passthrough inspection the connection is still anonymous, by this router's definition, for the whole exchange: `FilterContext#clientSaslAuthenticationSuccess` only fires after the last `SASL_AUTHENTICATE` response, so the router sees every request up to and including that one as anonymous.

SASL is stateful — a SCRAM exchange in particular is bound to nonces generated by whichever specific broker started it — so every request in one exchange must reach the same broker. The router cannot know a subject-based route before the subject is known, so it picks the alphabetically-first declared route on the connection's first pre-auth `SASL_HANDSHAKE`/`SASL_AUTHENTICATE` request and reuses that route for the rest of the exchange. This is independent of the route the subject resolves to once authenticated: a later request pins to whatever route the selector picks then, even if that differs from the route the SASL exchange used, and normal mid-connection route-change protection does not apply between the two.

**Deployments combining this router with SASL passthrough inspection must present the same credentials as valid on every configured route** — the router has no way to know in advance which route a given subject truly belongs to, so the pre-auth route is an arbitrary, fixed choice, not a routing decision.

## Metric

`kroxylicious_subject_router_rejected_total` — a counter of fail-closed rejections, tagged:

| Tag | Description |
|---|---|
| `virtual_cluster` | The virtual cluster name. |
| `router` | The router name, as declared in `routerDefinitions`. |
| `reason` | One of a fixed set: `anonymous`, `no-route`, `selector-error`, `route-changed`, `fan-out-error`. |

`reason` is a bounded enum, not derived from principal names or other unbounded input — this avoids both unbounded metric cardinality and leaking identity data into a metric label. Richer detail (e.g. which two routes conflicted on a mid-connection change) is only in the DEBUG-level log, never in the metric.

## Logging

The router never logs a principal name. If it logs the rejection reason at DEBUG, it includes `sessionId`, `virtualCluster`, `router`, and the free-text `reason` detail, but not the `Subject` itself.

## Preview gate

Routing (routers, routes, and multi-cluster dispatch generally, not just this router) is a preview feature. The runtime rejects `routerDefinitions` unless `KROXYLICIOUS_UNLOCK_ROUTING=true` is set. This applies to the routing API as a whole, not specifically to `SubjectRouter`.

## Known deviations from proposal #140

1. **Flat selector config.** The proposal nests `selector: { type, config }`; this prototype uses the flat `selector` + `selectorConfig` pair instead, matching the `kroxylicious-record-encryption` idiom (`kms`/`kmsConfig`, `selector`/`selectorConfig`) already established in this codebase.
2. **Response builder method name.** The proposal's text uses `andCloseConnection()`; the implemented Router API uses `withCloseConnection()`.
3. **Fail-closed rejection mechanism.** The proposal describes throwing a `SaslAuthenticationException`; the Router API exposes rejection via `RouterContext#respondWithError(..., Errors)`, not exception-based rejection, so this router calls `respondWithError(..., Errors.SASL_AUTHENTICATION_FAILED)` instead.
