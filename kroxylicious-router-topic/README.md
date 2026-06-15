# kroxylicious-router-topic

A Proof of Concept `Router` implementation that presents a single Kroxylicious virtual cluster backed by multiple independent Kafka clusters, routing requests to the correct backend based on topic name ownership.

See the [Limitations and future work](#limitations-and-future-work) at the end of this document.

## Overview

A standard Kroxylicious virtual cluster proxies a single Kafka cluster. The topic router removes that constraint: topics are partitioned across backends by name prefix, and the proxy decomposes batched requests, fans them out to the owning clusters, and recomposes the responses before returning them to the client. From the client's perspective the virtual cluster looks like one large Kafka cluster.

The router is plugged in as a `RouterFactory` via the Kroxylicious plugin system. Configuration maps topic name prefixes to named routes (each route corresponds to a backend cluster). A default route handles topics that match no prefix and all API keys that are not topic-addressed.

## Routing model

### Topic ownership

Each route declares topic ownership via prefixes, explicit topic names, or both. `PrefixTopicRoutingTable` enforces that prefixes on different routes are disjoint (no prefix may be a prefix of another on a different route), and that the same explicit topic name does not appear on multiple routes.

Routing precedence: explicit topic name > prefix match > default route. An explicit name can override a prefix on a different route, allowing exceptions to broad prefix rules.

If a topic matches no explicit name, no prefix, and a default route is configured, the topic is routed there. If no default route is configured, the topic is unroutable and gets a synthetic error response (`UNKNOWN_TOPIC_OR_PARTITION`).

### Subject-routed users

Each route can declare `subjects` — authenticated users whose coordinator-bound operations are locked to that route. A subject-routed user has all transactional, consumer group, and coordinator-discovery operations forwarded unconditionally to their assigned route. This avoids cross-cluster transactions and consumer groups entirely, relying on the client's own `FIND_COORDINATOR` flow and the proxy's bijective node ID mapping to reach the correct coordinator node.

Topic-addressed operations (PRODUCE, FETCH, etc.) still go through the normal leader-based routing handlers, but producer ID rewriting is skipped (a subject-routed user operates on a single backend, so one producer ID suffices).

METADATA and admin operations (CREATE_TOPICS, DELETE_TOPICS, CREATE_PARTITIONS) still fan out across all routes, so subject-routed users can see all topics and receive informative errors if they attempt cross-route operations.

Users not listed as `subjects` on any route are non-subject-routed: their topic-addressed requests are decomposed across routes by topic name, and coordinator-bound requests go to the default route.

### Routing modes

Requests are handled in one of three ways depending on the API key and whether the authenticated user is subject-routed:

**Static routing** — API keys not explicitly handled by the router are forwarded to the default route unchanged.

**Subject-routed forwarding** — For subject-routed users, coordinator-bound API keys (FIND_COORDINATOR, INIT_PRODUCER_ID, ADD_PARTITIONS_TO_TXN, ADD_OFFSETS_TO_TXN, TXN_OFFSET_COMMIT, END_TXN, OFFSET_COMMIT, OFFSET_FETCH, CONSUMER_GROUP_HEARTBEAT, CONSUMER_GROUP_DESCRIBE) are forwarded directly to the user's assigned route without decomposition or coordinator discovery.

**Dynamic decomposition** — Topic-addressed API keys (PRODUCE, FETCH, LIST_OFFSETS, METADATA, CREATE_TOPICS, DELETE_TOPICS, CREATE_PARTITIONS, DELETE_RECORDS, OFFSET_FOR_LEADER_EPOCH, DESCRIBE_CLUSTER) and API_VERSIONS are handled by dedicated per-API-key logic that may decompose the request by topic, fan out to multiple routes, and recompose the responses.

## Topology management

The router uses `TopologyService` (obtained from `RouterFactoryContext.topologyService()` during initialization) for all topology queries:

- **Leader lookup**: `topologyService.leaderOf(topicName, partitionIndex)` replaces the router's former internal `partitionLeaders` cache.
- **Cache warming**: `topologyService.ensureLeadersCached(topicsByRoute)` sends METADATA internally for uncached topics, batched one request per route.
- **Coordinator lookup**: `topologyService.coordinatorOf(route, keyType, key)` with fallback to `topologyService.discoverCoordinator(route, keyType, key)` replaces the former internal coordinator caches.
- **Staleness handling**: When the router observes `NOT_LEADER_OR_FOLLOWER` in a response, it calls `topologyService.invalidateRoute(route)`. No background METADATA refresh is fired — the client's own METADATA request (triggered by the error) repopulates the cache.

The `TopologyCache` backing the service is shared per router level (not per connection). This means a leader discovered by connection A is immediately available to connection B. The cache is populated as a side effect of METADATA responses flowing through `RoutingDecisionHandler.write()`.

## Request decomposition

Each dynamically routed API key has a `RequestDecomposer<Req, Resp>` implementation that handles:

1. **`decompose(request, table)`** -- splits the client's batched request into per-route sub-requests keyed by route name. Topics that are unroutable are excluded (handled separately). When all topics belong to a single route, the router short-circuits and forwards directly without fan-out.

2. **`recompose(responses, originalRequest)`** -- merges per-route sub-responses into a single response for the client.

Synthetic error responses for unroutable topics (and, where applicable, topics with rejected assignments) are generated separately and merged into the final response.

### Throttle time merging

When multiple backends return throttle times, the router takes the maximum across all responses. This ensures the client respects the most restrictive backend's rate limit.

## TopicId support

Several Kafka API keys transition from topic-name-based addressing to topic-ID-based addressing at certain versions (PRODUCE v13, FETCH v13, OFFSET_COMMIT v10, OFFSET_FETCH v10, DELETE_TOPICS v6). Topic IDs are cluster-specific -- the same topic on two independent clusters has different UUIDs.

TopicId resolution is handled by two internal filters installed by the runtime, not by the router itself. The router always sees topic names on both requests and responses.

### Request enrichment (`TopicIdRequestEnrichmentFilter`)

A per-connection filter on the frontend pipeline, positioned before user filters and routing. On the request path, it resolves topicIds to names from a local cache. On cache miss (e.g. the client learned topicIds from a different proxy instance, or has reconnected), it sends an internal METADATA-by-topicId request through the topology, waits for the response, caches the result, and then continues with the enriched request. On the response path, it learns topicId→name mappings from METADATA and other responses flowing back to the client. The cache is per-connection to avoid poisoning from subject-dependent name transforms in per-route filters.

### Response enrichment (`TopicIdResponseEnrichmentFilter`)

A shared filter installed immediately before the `routingTerminalHandler`. It enriches backend responses with topic names from a shared `ConcurrentHashMap` cache (one per virtual cluster, stored on `VirtualClusterModel`). On cache miss, it sends an internal METADATA-by-topicId to resolve the name. It is also installed in `buildFilters()` for non-routing configurations. Because this filter sees raw backend responses before any subject-dependent name transforms, the shared cache is safe.

### FetchSessionManager topicId propagation

The `FetchSessionManager` maintains client-side fetch session state using `TopicPartition` (name-based). When it creates new `FetchTopic` objects (in `buildFullRequestFromState` for session reconstruction, and `wrapForBackend` for incremental fetch), it preserves topicIds from a `clientTopicIds` map learned from incoming requests.

### Cache poisoning caveat

The response enrichment cache is shared and safe (it sees raw backend names). The request enrichment cache is per-connection (it sees post-transform names from per-route filters). If per-route filters transform names in a subject-dependent way, a shared request cache would be poisoned. A future optimisation could share the request cache when the topology is known to be "name-preserving", selectable via a filter property declaration.

## Version capping

Some API keys have structural changes unrelated to topicId that require version capping:

| API key | Capped to version | Reason |
|---|---|---|
| ADD_PARTITIONS_TO_TXN | v3 | v4+ is broker-only batch format |
| FIND_COORDINATOR | v3 | v4+ adds batched coordinator keys (KIP-699) |

The router intercepts `API_VERSIONS` responses and applies these caps via `ApiVersionsResponseTransformers.limitMaxVersionForApiKeys()`.

## Idempotent produce

Kafka's idempotent producer uses a `producerId` and `producerEpoch` (allocated by `INIT_PRODUCER_ID`) to enable exactly-once semantics. These IDs are scoped to a single broker cluster. When a non-subject-routed user's topics span multiple backends, each backend must allocate its own producer ID.

The router handles this with `ProducerIdManager` (for non-subject-routed users only; subject-routed users operate on a single backend and need no ID rewriting):

1. On `INIT_PRODUCER_ID`, the request is fanned out to all routes. Each backend allocates its own (producerId, epoch) pair.
2. The client-visible ID (from the default route) is returned to the client. The per-route mappings are stored with TTL-based eviction (default 7 days, matching Kafka's `transactional.id.expiration.ms`).
3. On `PRODUCE`, the router looks up the per-route mapping and uses `RecordBatchRewriter` to rewrite the producerId/epoch in record batch headers before forwarding to non-default routes.

If the mapping is missing (expired or never established), the router returns `UNKNOWN_PRODUCER_ID` to the client, which triggers a new `INIT_PRODUCER_ID` from the client.

## Fetch session management

The router implements bidirectional KIP-227 fetch session management via `FetchSessionManager`:

- **Client-facing (server-side)**: The router acts as a fetch session server for the downstream client. It maintains the full partition state and computes incremental responses.
- **Backend-facing (client-side)**: The router acts as a fetch session client for each upstream backend route, independently managing sessions per route.

These two sides are independent: a pre-v7 client (no session support) can still benefit from server-side sessions with backends, and vice versa.

`FetchSessionCache` bounds the total number of concurrent client-side sessions across all connections sharing a router. Eviction follows Kafka's KIP-227 strategy: stale sessions (idle beyond a configurable threshold) are evicted first; failing that, the smallest session that has existed long enough is replaced by a larger proposed session. If neither applies, the creation is declined and the client operates sessionless.

## Metadata merging

`MetadataDecomposer` handles the three METADATA request variants:

- **All-topics (`topics == null`)**: Sent to all routes. Responses are filtered so each route only contributes topics it owns (preventing phantom topics from auto-creation on the wrong cluster).
- **Broker-info-only (`topics` empty)**: Sent only to the default route.
- **Specific topics**: Grouped by owning route and fanned out.

Broker lists are unioned by `nodeId` across all responses. Cluster-level metadata (clusterId, controllerId) comes from the default route.

## Assignment rejection

For `CREATE_TOPICS` and `CREATE_PARTITIONS`, the router rejects requests that specify explicit broker assignments (replica placement). The virtual cluster presents a union of broker nodes from all backend clusters, so broker IDs chosen by the client may reference nodes from a different backend than the one the topic is routed to.

Rather than letting the backend reject these confusingly, the router detects them early and returns `INVALID_REPLICA_ASSIGNMENT` (error code 39, non-retriable). Topics without explicit assignments (automatic placement) are forwarded normally.

- Each rejection increments the `kroxylicious_routing_rejected_assignments_total` counter (tagged with `virtual_cluster` and `router`) and logs at WARN level.
- For `CreateTopics`: `topic.assignments().isEmpty()` determines automatic vs explicit (the collection is non-nullable).
- For `CreatePartitions`: `topic.assignments()` is nullable; null or empty means automatic.

## Metrics

| Metric | Type | Tags | Description |
|---|---|---|---|
| `kroxylicious_fetch_session_active_sessions` | Gauge | `virtual_cluster`, `router` | Currently active client-side fetch sessions |
| `kroxylicious_fetch_session_partitions_cached` | Gauge | `virtual_cluster`, `router` | Total cached partition count across sessions |
| `kroxylicious_fetch_session_evictions_total` | Counter | `virtual_cluster`, `router` | Cumulative fetch session evictions |
| `kroxylicious_routing_rejected_assignments_total` | Counter | `virtual_cluster`, `router` | Topics rejected for explicit broker assignments |

## Configuration

Each route defines what it owns: topic prefixes, explicit topic names, and subject-routed users.

```yaml
router:
  type: TopicPartitionRouterFactory
  config:
    defaultRoute: cluster-a          # optional; route for unmatched topics
    routes:
      - name: cluster-a
        topicPrefixes: ["orders.", "payments."]
      - name: cluster-b
        topicPrefixes: ["analytics.", "logs."]
        topics: ["special-topic"]    # optional; explicit topic names
        subjects: [bob]              # optional; subject-routed users
    producerIdTtl: PT168H            # optional; default 7 days
    maxFetchSessionCacheSlots: 1000  # optional
    minFetchSessionEviction: PT2M    # optional; default 120 seconds
```

Subject-routed users have all coordinator-bound operations (transactions, consumer groups, coordinator discovery) forwarded to their assigned route. Topic-addressed operations still use leader-based routing within the subject's route. METADATA and admin operations fan out across all routes so the user can see all topics.

## Limitations and future work

- **Inadequate testing**: **DO NOT RELY ON THIS CODE IN ANY WAY WHATSOEVER**. Really, we mean it. It's not been reviewed. It's been only lightly tested, in very limited environments. 
- **No classic consumer groups**: Only the "new" KIP-848 consumer group protocol is supported.
- **TopicId support is new and lightly tested**: TopicId-bearing API versions (PRODUCE v13, FETCH v13, OFFSET_COMMIT v10, OFFSET_FETCH v10, DELETE_TOPICS v6) are supported via internal enrichment filters, but this has not been extensively tested in production environments.
- **Cross-route transactions**: A transactional producer whose topics span multiple backends will not get correct exactly-once semantics. Subject-based routing ensures each user's transactions target a single backend.
- **Cross-route consumer groups**: Consumer groups whose subscriptions span routes will see incomplete behaviour. Subject-based routing ensures each user's groups target a single backend.
- **No topic migration**: Once a topic is assigned to a route by prefix or explicit name, it cannot be moved without reconfiguration and data migration.
- **Authentication termination required**: Because of the use of Subject-based routing the router requires a non-anonymous `Subject` to make routing decisions for transactional producers and consumer groups. Worse, most SASL mechanisms do not work with the fan-out concept. This means that SASL termination is more-or-less required.
- **No support for new, or even newish, Kafka functionality**: Including Share Groups, Streams Groups and 2 Phase Commit.
- **Impolite `FETCH` session closure**: We don't invalidate our session on the broker when our client disconnects, so it could consume resources until it expires.
