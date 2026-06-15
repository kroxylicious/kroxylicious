/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;

/**
 * Opt-in topology cache for routers that need leader, coordinator,
 * or topic ID resolution.
 *
 * <p>Routers that need topology information obtain a
 * {@code TopologyService} from
 * {@link RouterFactoryContext#topologyService()} during
 * {@link RouterFactory#initialize}. The runtime creates the
 * underlying cache on first request and populates it from METADATA
 * responses that flow through the routing pipeline. Routers that
 * never call {@code topologyService()} pay no cost.</p>
 *
 * <p>The cache is shared per router level (not per connection),
 * so all connections through the same router level share the same
 * topology view. The cache is thread-safe.</p>
 */
public interface TopologyService {

    /**
     * Resolves topic IDs to topic names, batching cache misses into
     * a single METADATA request.
     *
     * <p>Returns a map containing an entry for each topic ID that
     * was successfully resolved. Topic IDs that could not be resolved
     * (e.g. deleted topics) are absent from the returned map.</p>
     *
     * @param topicIds the topic IDs to resolve
     * @return a stage that completes with the resolved mappings
     */
    CompletionStage<Map<Uuid, String>> topicNames(Set<Uuid> topicIds);

    /**
     * Returns the cached leader for a topic-partition, or empty if
     * the leader is not cached.
     *
     * <p>Call {@link #ensureLeadersCached} first to warm the cache
     * for the topics you need.</p>
     *
     * @param topicName the topic name
     * @param partitionIndex the partition index
     * @return the leader's virtual node, or empty if not cached
     */
    Optional<VirtualNode> leaderOf(String topicName, int partitionIndex);

    /**
     * Ensures leaders are cached for the given topics on the given
     * routes, sending METADATA requests as needed. Uncached topics
     * are batched into one METADATA request per route.
     *
     * <p>On success, subsequent calls to {@link #leaderOf} for the
     * requested topics will return non-empty. On failure (e.g. METADATA
     * request fails or returns topic-level errors), the stage completes
     * exceptionally and topics with errors are not cached.</p>
     *
     * <p>This does not block the event loop — it sends METADATA
     * requests asynchronously and the returned stage completes when
     * the responses arrive.</p>
     *
     * @param topicsByRoute map from route name to set of topic names
     * @return a stage that completes when the cache is warm
     */
    CompletionStage<Void> ensureLeadersCached(Map<String, Set<String>> topicsByRoute);

    /**
     * Returns the cached coordinator for the given key, or empty if
     * not cached.
     *
     * @param route the route name
     * @param keyType 0 for group, 1 for transaction
     * @param key the group or transaction ID
     * @return the coordinator's virtual node, or empty if not cached
     */
    Optional<VirtualNode> coordinatorOf(String route, byte keyType, String key);

    /**
     * Discovers and caches a coordinator by sending METADATA (if needed)
     * then FIND_COORDINATOR to the given route.
     *
     * @param route the route name
     * @param keyType 0 for group, 1 for transaction
     * @param key the group or transaction ID
     * @return a stage that completes with the coordinator's virtual node
     */
    CompletionStage<VirtualNode> discoverCoordinator(String route, byte keyType, String key);

    /**
     * Returns full partition info (leader, replicas, ISR) for a
     * topic-partition, or empty if not cached.
     *
     * <p>Useful for follower-fetch / AZ-aware routing where the
     * router needs to find an in-sync replica in the local rack.</p>
     *
     * @param topicName the topic name
     * @param partitionIndex the partition index
     * @return the partition info, or empty if not cached
     */
    Optional<PartitionInfo> partitionInfoFor(String topicName, int partitionIndex);

    /**
     * Returns broker metadata (host, port, rack) for a virtual node,
     * or empty if not cached.
     *
     * @param node the virtual node
     * @return the broker info, or empty if not cached
     */
    Optional<BrokerInfo> brokerInfo(VirtualNode node);

    /**
     * Coarse invalidation: clears all partition info, coordinators,
     * and broker info for a route.
     *
     * <p>Topic ID to name mappings are not cleared (they are stable
     * within a cluster).</p>
     *
     * <p>Called by the router when it observes staleness indicators
     * (e.g. {@code NOT_LEADER_OR_FOLLOWER}, {@code NOT_COORDINATOR})
     * in responses. No background refresh is fired — the client
     * drives the refresh via its own METADATA request.</p>
     *
     * @param route the route to invalidate
     */
    void invalidateRoute(String route);
}
