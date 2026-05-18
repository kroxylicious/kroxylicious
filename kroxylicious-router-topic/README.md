# kroxylicious-router-topic

A `Router` implementation that presents a single Kroxylicious virtual cluster backed by multiple independent Kafka clusters, routing requests to the correct backend based on topic name ownership.

## Overview

A standard Kroxylicious virtual cluster proxies a single Kafka cluster. The topic router removes that constraint: topics are partitioned across backends by name prefix, and the proxy decomposes batched requests, fans them out to the owning clusters, and recomposes the responses before returning them to the client. From the client's perspective the virtual cluster looks like one large Kafka cluster.

The router is plugged in as a `RouterFactory` via the Kroxylicious plugin system. Configuration maps topic name prefixes to named routes (each route corresponds to a backend cluster). A default route handles topics that match no prefix and all API keys that are not topic-addressed.

## Routing model

### Prefix-based topic ownership

Each route declares one or more topic name prefixes. `PrefixTopicRoutingTable` enforces that prefixes on different routes are disjoint (no prefix may be a prefix of another on a different route). Lookup is O(log n + k) via binary search over sorted prefixes.

If a topic matches no prefix and a default route is configured, the topic is routed there. If no default route is configured, the topic is unroutable and gets a synthetic error response (`UNKNOWN_TOPIC_OR_PARTITION`).

### Static vs dynamic routing

Most Kafka API keys are *statically routed* to the default route. The router only needs to inspect and decompose requests for API keys that contain topic or partition addressing:

| API key | Routing | Notes |
|---|---|---|
| API_VERSIONS | Dynamic | Intercepted for version capping |
| PRODUCE | Dynamic | Fan-out by topic, producer ID rewriting |
| INIT_PRODUCER_ID | Dynamic | Fanned out to all routes for ID mapping |
| METADATA | Dynamic | Fan-out, broker union, phantom filtering |
| FETCH | Dynamic | Fan-out by topic, bidirectional session management |
| LIST_OFFSETS | Dynamic | Fan-out by topic |
| OFFSET_COMMIT | Dynamic | Fan-out by topic |
| CREATE_TOPICS | Dynamic | Fan-out by topic, assignment rejection |
| DELETE_TOPICS | Dynamic | Fan-out by topic name (v0-5 wire format) |
| CREATE_PARTITIONS | Dynamic | Fan-out by topic, assignment rejection |
| DELETE_RECORDS | Dynamic | Fan-out by topic |
| Everything else | Static | Forwarded to the default route unchanged |

## Request decomposition

Each dynamically routed API key has a `RequestDecomposer<Req, Resp>` implementation that handles:

1. **`decompose(request, table)`** -- splits the client's batched request into per-route sub-requests keyed by route name. Topics that are unroutable are excluded (handled separately). When all topics belong to a single route, the router short-circuits and forwards directly without fan-out.

2. **`recompose(responses, originalRequest)`** -- merges per-route sub-responses into a single response for the client.

Synthetic error responses for unroutable topics (and, where applicable, topics with rejected assignments) are generated separately and merged into the final response.

### Throttle time merging

When multiple backends return throttle times, the router takes the maximum across all responses. This ensures the client respects the most restrictive backend's rate limit.

## Version capping

Several Kafka API keys transition from topic-name-based addressing to topic-ID-based addressing at certain versions. Topic IDs are cluster-specific -- the same topic on two independent clusters has different UUIDs. The router must force name-based addressing to maintain routing correctness.

| API key | Capped to version | Reason |
|---|---|---|
| PRODUCE | v12 | v13 (KIP-516) uses TopicId instead of Name |
| FETCH | v12 | v13 uses TopicId |
| OFFSET_COMMIT | v9 | v10+ uses TopicId |
| OFFSET_FETCH | v9 | v10+ uses TopicId |
| DELETE_TOPICS | v5 | v6+ uses TopicId |

The router intercepts `API_VERSIONS` responses and applies these caps via `ApiVersionsResponseTransformers.limitMaxVersionForApiKeys()`, so clients negotiate down to versions that use topic names on the wire.

## Idempotent produce

Kafka's idempotent producer uses a `producerId` and `producerEpoch` (allocated by `INIT_PRODUCER_ID`) to enable exactly-once semantics. These IDs are scoped to a single broker cluster. When topics span multiple backends, each backend must allocate its own producer ID.

The router handles this with `ProducerIdManager`:

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

```yaml
router:
  type: TopicPartitionRouterFactory
  config:
    defaultRoute: cluster-a          # optional; route for unmatched topics
    topicRoutes:
      - route: cluster-a
        topicPrefixes: ["orders.", "payments."]
      - route: cluster-b
        topicPrefixes: ["analytics.", "logs."]
    producerIdTtl: PT168H            # optional; default 7 days
    maxFetchSessionCacheSlots: 1000  # optional
    minFetchSessionEviction: PT2M    # optional; default 120 seconds
```

## Limitations and future work

- **Transactions**: Transactional produce and commit are not yet supported across routes. A transactional producer whose topics span multiple backends will not get correct exactly-once semantics.
- **Consumer group coordination**: GROUP_COORDINATOR, JOIN_GROUP, SYNC_GROUP, HEARTBEAT, LEAVE_GROUP, and OFFSET_FETCH are not yet decomposed. Consumer groups whose subscriptions span routes will see incomplete behaviour.
- **Single proxy instance**: There is no control plane for coordinating multiple proxy instances. Partition leadership is determined by the backends.
- **No topic migration**: Once a topic is assigned to a route by prefix, it cannot be moved without reconfiguration and data migration.
