# Subject Router

Routes each client connection to a downstream cluster based on the client's authenticated identity, its `client.id`, or both.
Implements [design proposal #140](https://github.com/kroxylicious/design/pull/140).

This is a preview feature, gated behind `KROXYLICIOUS_UNLOCK_ROUTING=true` (see [Preview gate](#preview-gate)).

## How it routes

The router matches the connection's authenticated `Subject` and pinned `client.id` against an ordered list of rules (`ruleMatch`), then pins the connection to the resolved route:

1. **A rule matches** — `ruleMatch` evaluates its `rules` in declaration order and uses the route of the first matching rule. A rule's `hasPrincipals` can only match an authenticated subject; a rule's `hasClientId` can match before, or without, authentication, since `client.id` comes from the request header, not the subject. The first pick pins the connection; a later request that resolves to a *different* route closes the connection rather than switching clusters.
2. **No rule matches, subject authenticated** — routed to `defaultRoute` if configured, otherwise rejected fail-closed.
3. **No rule matches, subject anonymous, `API_VERSIONS`** — before a client authenticates (mTLS handshake pending, or pre-SASL), the router fans the request out to every configured route and returns the intersection of supported API versions, so the client negotiates a version range every downstream cluster actually supports. See [Anonymous `API_VERSIONS` fan-out](#anonymous-api_versions-fan-out).
4. **No rule matches, subject anonymous, anything else, including `SASL_HANDSHAKE`/`SASL_AUTHENTICATE`** — rejected fail-closed. `defaultRoute` never applies here: an anonymous connection can only be routed by an explicit `hasClientId` rule naming the exact value it accepts. SASL passthrough is not supported: a passthrough-inferred identity is asserted by the backend rather than verified by the proxy, so the router has no verified subject to route on. Deferred to future work.

If the connection is to a broker-specific gateway endpoint (`RouterContext#virtualNode()` non-empty), the router sends there rather than to an arbitrary node of the chosen route, so a client that dialed a specific broker keeps talking to it.

Fail-closed rejections use `Errors.SASL_AUTHENTICATION_FAILED`; the runtime maps it to a per-API error response and the connection is closed.

## Configuration

```yaml
routerDefinitions:
  - name: subject-router
    type: SubjectRouter
    config:
      ruleMatch:
        defaultRoute: team-a         # optional; authenticated-but-unmapped subjects route here
        rules:
          - hasPrincipals:
              - withType: io.kroxylicious.proxy.authentication.User
                withName: "CN=alice"
            useRoute: team-a
          - hasPrincipals:
              - withType: io.kroxylicious.proxy.authentication.User
                withName: "CN=bob"
            useRoute: team-a
          - hasPrincipals:
              - withType: io.kroxylicious.proxy.authentication.User
                withName: "CN=carol"
            useRoute: team-b
          - hasPrincipals:
              - withType: com.example.Role
                withName: developer
              - withType: com.example.Role
                withName: team-a       # two pairs of the same type: subject must carry both roles
            useRoute: team-a
          - hasClientId: demo-app      # no principal required; matches before/without authentication
            useRoute: team-a
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
| `ruleMatch.rules` | ordered list of `{hasPrincipals, hasClientId, useRoute}` | `hasPrincipals` is a list of `{withType, withName}` pairs, `withType` a fully-qualified `Principal` implementation name and `withName` a required principal name; the rule matches only if the subject carries, for every listed pair, a principal of that type with that name. It's a list rather than a map keyed by type because a YAML mapping can't have two keys the same: expressing "both role `developer` and role `team-a`" needs two pairs with the same `withType`. The check is a membership test (`Subject.allPrincipalsOfType(withType)`), so it works the same whether a subject can carry at most one principal of that type (like `User`) or several (like a role/group type). `hasClientId` is a required value for the connection's pinned `client.id`. A rule needs at least one of `hasPrincipals`/`hasClientId`; when both are given, the subject must satisfy every `hasPrincipals` pair *and* the pinned `client.id` must equal `hasClientId`. `useRoute` names a route declared in the router's `routes`. The first matching rule wins. Startup fails if a rule has neither predicate, if two rules have identical predicates (`hasPrincipals` compared order-insensitively), or if a `useRoute` is not declared. |
| `ruleMatch.defaultRoute` | string, optional | Route for an authenticated subject whose principals and `client.id` match no rule. Never applies to an anonymous subject — see [How it routes](#how-it-routes). If omitted, such a subject is rejected fail-closed. |

A `hasPrincipals` pair is checked via `subject.allPrincipalsOfType(withType).stream().anyMatch(p -> p.name().equals(withName))` — for mTLS against the default `User` type this is the client certificate's subject DN as rendered by `X500Principal.getName(X500Principal.RFC1779, ...)` (with `emailAddress` mapped from its OID); for SASL termination it is the authenticated username.

### `client.id` matching

`client.id` is read from `RequestHeaderData.clientId()`, captured from the connection's first request (including a pre-authentication `API_VERSIONS`) and pinned for the life of the connection — never re-read, anticipating [KIP-1313][kip-1313], which stops sending `client.id` on requests after the first. A client that sends a different `client.id` later on the same connection has no effect; the router keeps using the pinned value.

**`client.id` provides no isolation.** It is a client-supplied, unverified value: any client can present any `client.id`. A rule with only `hasClientId` is a convenience partition for demos, quickstarts, and trusted-network deployments, not a security control — any client can bypass or impersonate it by sending a different value. A rule combining `hasPrincipals` and `hasClientId` is exactly as strong as its `hasPrincipals` part; `hasClientId` only narrows which of that principal's own connections the rule matches, adding no further guarantee. Admins wanting tenant isolation should rely on `hasPrincipals` behind mTLS or SASL termination, not `hasClientId`.

[kip-1313]: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/406624038/KIP-1313+Client+instance+ID+in+all+request+headers

## Anonymous `API_VERSIONS` fan-out

`SubjectRouter` never declares static routes (`staticRoutes()` is always empty), because which downstream cluster a connection belongs to depends on identity that is not known until routing time — unlike a router that always sends a given API key to the same fixed place. This means `API_VERSIONS` is dynamically routed like everything else, and the router itself must handle it specially for the pre-authentication case: the runtime does not compute a cross-route version intersection on a router's behalf.

This fan-out only happens when `API_VERSIONS` arrives on a connection with no resolved route: the subject is still anonymous and no `hasClientId` rule matches the pinned `client.id`. If a rule already resolves the route — a matching `hasClientId` rule, or an mTLS-established subject — `API_VERSIONS` is forwarded directly to that route like any other request, with no fan-out.

For an unresolved connection's `API_VERSIONS` request, the router sends the request to every declared route concurrently and combines the responses:

- An API key survives only if every route's response includes it, with the version range narrowed to the overlap (`minVersion = max(...)`, `maxVersion = min(...)`). An API key present in only some routes' responses is dropped.
- If any route's response carries a non-zero top-level `errorCode`, the whole request is rejected fail-closed rather than returning a partial intersection.
- Feature blocks (`supportedFeatures`, `finalizedFeatures`, `finalizedFeaturesEpoch`, `zkMigrationReady`) are not intersected — the first route's response is copied as-is. This is a simplification; routes with materially different feature sets are not supported by this fan-out.
- The connection is not pinned to any route during fan-out — pinning happens on the first forward to a resolved route.

## Metric

`kroxylicious_subject_router_rejected_total` — a counter of fail-closed rejections, tagged:

| Tag | Description |
|---|---|
| `virtual_cluster` | The virtual cluster name. |
| `router` | The router name, as declared in `routerDefinitions`. |
| `reason` | One of a fixed set: `anonymous`, `unmapped`, `route-changed`, `fan-out-error`. |

`reason` is a bounded enum, not derived from principal names or other unbounded input — this avoids both unbounded metric cardinality and leaking identity data into a metric label. Richer detail (e.g. which two routes conflicted on a mid-connection change) is only in the DEBUG-level log, never in the metric.

## Logging

The router never logs a principal name. If it logs the rejection reason at DEBUG, it includes `sessionId`, `virtualCluster`, `router`, and the free-text `reason` detail, but not the `Subject` itself.

## Preview gate

Routing (routers, routes, and multi-cluster dispatch generally, not just this router) is a preview feature. The runtime rejects `routerDefinitions` unless `KROXYLICIOUS_UNLOCK_ROUTING=true` is set. This applies to the routing API as a whole, not specifically to `SubjectRouter`.

## Known deviations from proposal #140

1. **Response builder method name.** The proposal's text uses `andCloseConnection()`; the implemented Router API uses `withCloseConnection()`.
2. **Fail-closed rejection mechanism.** The proposal describes throwing a `SaslAuthenticationException`; the Router API exposes rejection via `RouterContext#respondWithError(..., Errors)`, not exception-based rejection, so this router calls `respondWithError(..., Errors.SASL_AUTHENTICATION_FAILED)` instead.
