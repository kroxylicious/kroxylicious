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
 * broker, or topic ID information.
 *
 * <p>Routers that need topology information obtain a
 * {@code TopologyService} from
 * {@link RouterFactoryContext#topologyService()} during
 * {@link RouterFactory#initialize}. The runtime creates the
 * underlying cache on first request. Routers that never call
 * {@code topologyService()} pay no cost.</p>
 *
 * <h2>Discovery methods</h2>
 *
 * <p>The three discovery methods — {@link #leaders}, {@link #coordinators},
 * {@link #topicNames} — are async and return self-contained result
 * objects. They may send requests internally to warm the cache
 * (METADATA for leaders and topic names, FIND_COORDINATOR for
 * coordinators). The results are valid immediately upon completion
 * and do not require further cache queries.</p>
 *
 * <h2>Cache population</h2>
 *
 * <p>The cache is populated as a <b>side effect</b> of responses
 * flowing through the routing pipeline:</p>
 * <ul>
 *   <li>METADATA responses populate partition leaders, replicas,
 *       ISR, broker info, and topicId→name mappings.</li>
 *   <li>FIND_COORDINATOR responses populate coordinator mappings,
 *       using request-side context (keyType, key) carried by the
 *       runtime's {@code PendingResponse}.</li>
 * </ul>
 * <p>By the time a discovery method's {@code CompletionStage}
 * completes, the cache is guaranteed to reflect the response.</p>
 *
 * <h2>Cache scope</h2>
 *
 * <p>The cache is shared per router level (not per connection),
 * so all connections through the same router level share the same
 * topology view. The cache is thread-safe.</p>
 */
public interface TopologyService {

    /**
     * Discovers partition leaders for the given topics on the given
     * routes, sending METADATA requests as needed. Uncached topics
     * are batched into one METADATA request per route.
     *
     * <p>The returned {@link PartitionLeaders} is a self-contained
     * snapshot — callers should use it directly rather than querying
     * the cache separately.</p>
     *
     * @param topicsByRoute map from route name to set of topic names
     * @return a stage that completes with the discovered leaders
     */
    CompletionStage<PartitionLeaders> leaders(Map<String, Set<String>> topicsByRoute);

    /**
     * Discovers coordinators for the given keys on the given route,
     * sending METADATA (if needed) then FIND_COORDINATOR. Supports
     * batched lookup matching the FIND_COORDINATOR v4+ protocol.
     *
     * <p>The returned {@link Coordinators} is a self-contained
     * snapshot — callers should use it directly rather than querying
     * the cache separately.</p>
     *
     * @param route the route name
     * @param keyType 0 for group, 1 for transaction
     * @param keys the group or transaction IDs to discover
     * @return a stage that completes with the discovered coordinators
     */
    CompletionStage<Coordinators> coordinators(String route, byte keyType, Set<String> keys);

    /**
     * Resolves topic IDs to topic names, batching cache misses into
     * a single METADATA request per route.
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
     * Returns full partition info (leader, replicas, ISR) for a
     * topic-partition, or empty if not cached.
     *
     * <p>This is a supplementary lookup for use cases like
     * follower-fetch / AZ-aware routing where the router needs
     * replica and rack information beyond what {@link PartitionLeaders}
     * provides. If this returns empty, the router can fall back to
     * the leader from {@link PartitionLeaders}.</p>
     *
     * @param topicName the topic name
     * @param partitionIndex the partition index
     * @return the partition info, or empty if not cached
     */
    Optional<PartitionInfo> partitionInfo(String topicName, int partitionIndex);

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
     * drives the refresh via its own METADATA request, which
     * repopulates the cache as a side effect.</p>
     *
     * @param route the route to invalidate
     */
    void invalidateRoute(String route);
}
