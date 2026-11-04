# Subject Router

Routes each client connection to a downstream cluster based on the connection's authenticated identity.
Implements [design proposal #140](https://github.com/kroxylicious/design/pull/140).

This is a preview feature, gated behind `KROXYLICIOUS_UNLOCK_ROUTING=true` (see [Preview gate](#preview-gate)).

## How it routes

The router matches the connection's authenticated `Subject` against a static principal-name-to-route mapping (`singularPrincipalMatch`), then pins the connection to the resolved route:

1. **Authenticated request** — the mapping picks a route for the subject. The first pick pins the connection; a later request that resolves to a *different* route closes the connection rather than switching clusters.
2. **Anonymous `API_VERSIONS`** — before a client authenticates (mTLS handshake pending, or pre-SASL), the router fans the request out to every configured route and returns the intersection of supported API versions, so the client negotiates a version range every downstream cluster actually supports. See [Anonymous `API_VERSIONS` fan-out](#anonymous-api_versions-fan-out).
3. **Everything else anonymous, including `SASL_HANDSHAKE`/`SASL_AUTHENTICATE`** — rejected fail-closed. SASL passthrough is not supported: a passthrough-inferred identity is asserted by the backend rather than verified by the proxy, so the router has no verified subject to route on. Deferred to future work.
4. **No route for the subject** (no principal of the configured type, or its name is absent from `routeMapping`, and no `defaultRoute` is configured) — rejected fail-closed.

If the connection is to a broker-specific gateway endpoint (`RouterContext#virtualNode()` non-empty), the router sends there rather than to an arbitrary node of the chosen route, so a client that dialed a specific broker keeps talking to it.

Fail-closed rejections use `Errors.SASL_AUTHENTICATION_FAILED`; the runtime maps it to a per-API error response and the connection is closed.

## Configuration

```yaml
routerDefinitions:
  - name: subject-router
    type: SubjectRouter
    config:
      singularPrincipalMatch:
        type: io.kroxylicious.proxy.authentication.User
        defaultRoute: team-a   # optional
        routeMapping:
          - principalName: "CN=alice"
            route: team-a
          - principalName: "CN=bob"
            route: team-a
          - principalName: "CN=carol"
            route: team-b
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
| `singularPrincipalMatch.type` | string | Fully-qualified name of a `Principal` implementation annotated `@Unique` or `@SingularPrincipal`. The factory rejects a non-singular type at startup. |
| `singularPrincipalMatch.routeMapping` | list of `{principalName, route}` | Each entry pairs a principal name with a route declared in the router's `routes`. Startup fails if the same `principalName` appears more than once, or if a `route` is not declared. |
| `singularPrincipalMatch.defaultRoute` | string, optional | Route for an authenticated subject whose principal name is absent from `routeMapping`, or that carries no principal of `type` at all. Omit to reject such a subject fail-closed. |

The principal matched is `subject.uniquePrincipalOfType(type).map(Principal::name)` — for mTLS against the default `User` type this is the client certificate's subject DN as rendered by `X500Principal.getName(X500Principal.RFC1779, ...)` (with `emailAddress` mapped from its OID); for SASL termination it is the authenticated username.

## Anonymous `API_VERSIONS` fan-out

`SubjectRouter` never declares static routes (`staticRoutes()` is always empty), because which downstream cluster a connection belongs to depends on identity that is not known until routing time — unlike a router that always sends a given API key to the same fixed place. This means `API_VERSIONS` is dynamically routed like everything else, and the router itself must handle it specially for the pre-authentication case: the runtime does not compute a cross-route version intersection on a router's behalf.

For an anonymous connection's `API_VERSIONS` request, the router sends the request to every declared route concurrently and combines the responses:

- An API key survives only if every route's response includes it, with the version range narrowed to the overlap (`minVersion = max(...)`, `maxVersion = min(...)`). An API key present in only some routes' responses is dropped.
- If any route's response carries a non-zero top-level `errorCode`, the whole request is rejected fail-closed rather than returning a partial intersection.
- Feature blocks (`supportedFeatures`, `finalizedFeatures`, `finalizedFeaturesEpoch`, `zkMigrationReady`) are not intersected — the first route's response is copied as-is. This is a simplification; routes with materially different feature sets are not supported by this fan-out.
- The connection is not pinned to any route during fan-out — pinning happens on the first *authenticated* request.

## Metric

`kroxylicious_subject_router_rejected_total` — a counter of fail-closed rejections, tagged:

| Tag | Description |
|---|---|
| `virtual_cluster` | The virtual cluster name. |
| `router` | The router name, as declared in `routerDefinitions`. |
| `reason` | One of a fixed set: `anonymous`, `no-route`, `route-changed`, `fan-out-error`. |

`reason` is a bounded enum, not derived from principal names or other unbounded input — this avoids both unbounded metric cardinality and leaking identity data into a metric label. Richer detail (e.g. which two routes conflicted on a mid-connection change) is only in the DEBUG-level log, never in the metric.

## Logging

The router never logs a principal name. If it logs the rejection reason at DEBUG, it includes `sessionId`, `virtualCluster`, `router`, and the free-text `reason` detail, but not the `Subject` itself.

## Preview gate

Routing (routers, routes, and multi-cluster dispatch generally, not just this router) is a preview feature. The runtime rejects `routerDefinitions` unless `KROXYLICIOUS_UNLOCK_ROUTING=true` is set. This applies to the routing API as a whole, not specifically to `SubjectRouter`.

## Known deviations from proposal #140

1. **Response builder method name.** The proposal's text uses `andCloseConnection()`; the implemented Router API uses `withCloseConnection()`.
2. **Fail-closed rejection mechanism.** The proposal describes throwing a `SaslAuthenticationException`; the Router API exposes rejection via `RouterContext#respondWithError(..., Errors)`, not exception-based rejection, so this router calls `respondWithError(..., Errors.SASL_AUTHENTICATION_FAILED)` instead.
