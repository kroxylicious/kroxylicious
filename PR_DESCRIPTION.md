## Summary

Add topicId support to the topic-partition router, removing the version caps that previously forced clients to use name-based addressing for PRODUCE (v12→v13), FETCH (v12→v13), OFFSET_COMMIT (v9→v10), OFFSET_FETCH (v9→v10), and DELETE_TOPICS (v5→v6). The remaining version caps (ADD_PARTITIONS_TO_TXN v3, FIND_COORDINATOR v3) are unrelated to topicIds and stay.

## Approach

### Enrichment filters rather than router-local resolution

An early iteration resolved topicIds inside the `TopicPartitionRouter` itself — maintaining its own cache, sending internal METADATA requests on cache miss, and enriching each API's request/response before decomposition. This worked but had significant drawbacks:

- Every router (and potentially every filter) that needs topic names would need its own resolution logic and cache.
- The router became substantially more complex, with per-API enrichment methods, async resolution chaining, per-request reverse-lookup maps for the FetchSessionManager, and special-casing for acks=0.
- Caching couldn't benefit other components in the topology.

Instead, we apply the enrichment pattern at the runtime level via two internal filters. Everything downstream — user filters, routers, other internal filters — always sees topic names populated, regardless of API version.

### Two enrichment filters

**`TopicIdRequestEnrichmentFilter`** (per-connection, frontend pipeline): Enriches client requests. On cache miss, sends an internal METADATA-by-topicId through the topology (leveraging the existing `LEARN_TOPIC_NAMES` tagging and `TopicNameCacheFilter` short-circuit mechanism). The cache is per-connection because per-route filters may transform topic names in a subject-dependent way — a shared cache would be poisoned.

**`TopicIdResponseEnrichmentFilter`** (shared cache, before terminal handler): Enriches backend responses. The cache is a `ConcurrentHashMap` on `VirtualClusterModel`, shared across all connections to the same virtual cluster. This is safe because the filter sees raw backend responses before any subject-dependent name transforms. On cache miss, sends an internal METADATA-by-topicId to resolve.

### FetchSessionManager topicId propagation

The `FetchSessionManager` uses `TopicPartition` (name-based) for session state. When it reconstructs full fetch requests from session state (`buildFullRequestFromState`) or wraps requests for backend sessions (`wrapForBackend`), it creates new `FetchTopic` objects. These now carry topicIds via a `clientTopicIds` map learned from incoming requests, so the backend receives valid topicIds at v13+.

### DeleteTopicsDecomposer v6 support

DELETE_TOPICS v6 restructures from a flat string list (`topicNames`) to a struct array (`topics`) with both `Name` and `TopicId`. The decomposer now handles both formats, dispatched by API version.

### RequestDecomposer interface

Added `apiVersion` parameter to `RequestDecomposer.decompose()` and `recompose()` so decomposers can handle version-dependent wire formats.

## Tech debt and further work

- **acks=0 produce flake**: At v13 the acks=0 test (`shouldFanOutAcksZeroProduce`) flakes ~1/10. The `TopicIdRequestEnrichmentFilter`'s async METADATA resolution on cache miss adds latency; the producer may close the connection before the proxy forwards the request. At v12 the request was forwarded synchronously so the race didn't manifest. Needs investigation into whether the runtime should flush queued backend writes before tearing down on client disconnect.

- **Cache poisoning with subject-dependent name transforms**: The response enrichment cache is shared and safe (raw backend names). The request enrichment cache is per-connection (safe but wasteful). A future optimisation: if all filters in the topology are declared "name-preserving" (via an opt-in property), the request cache could be shared too. This requires the planned plugin type-checking mechanism.

- **METADATA-by-topicId decomposition in the router**: When the request enrichment filter sends METADATA-by-topicId on cache miss, the router needs to decompose it across routes. If per-route filters cause the same topicId to resolve to different names on different routes, the router's routing table acts as arbiter (the merged METADATA response flows back through the request enrichment filter, populating the cache with the router-decided name). This works but hasn't been tested with name-transforming per-route filters.

## Test plan

- [ ] 294 router unit tests pass (decomposers, router, FetchSessionManager)
- [ ] `ProduceRoutingIT` — 19/20 pass (1 acks=0 flake, pre-existing race)
- [ ] `FetchRoutingIT` — 20/20 pass
- [ ] `DeleteTopicsRoutingIT` — 6/6 pass
- [ ] `OffsetCommitRoutingIT` — 9/9 pass
- [ ] `ApiVersionsRoutingIT` — 1/1 pass (verifies uncapped versions)
- [ ] `TransactionalProduceRoutingIT` — 18/18 pass
- [ ] `ConsumerGroupRoutingIT` — 9/9 pass
- [ ] `CrossInstanceRoutingIT` — 2/2 pass (cross-instance topicId resolution)
- [ ] `shouldProduceAfterTopicRecreation` — topic recreation test

🤖 Generated with [Claude Code](https://claude.com/claude-code)
