/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.Errors;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Thread-safe cache of cluster topology data, populated from METADATA
 * and FIND_COORDINATOR responses. Shared per router level (not per
 * connection) — all connections through the same router level share
 * the same virtual node ID space.
 *
 * <p>Only METADATA responses populate partition and broker data.
 * DESCRIBE_CLUSTER is intentionally excluded to avoid inconsistency
 * between broker lists and partition leader assignments.</p>
 *
 * <p><b>Authorization safety:</b> the cache is not scoped by
 * authenticated subject. When Kafka ACLs filter METADATA responses,
 * different connections contribute different subsets of topics. The
 * cache uses additive (union) semantics: each response adds or
 * updates entries for topics present, without removing entries for
 * topics absent from the response. This is safe because the cache
 * is used only for routing (leader selection, coordinator discovery),
 * not authorization. Backend brokers enforce ACLs on the actual
 * requests (PRODUCE, FETCH, etc.).</p>
 */
public class TopologyCache {

    private final ConcurrentHashMap<PartitionKey, PartitionEntry> partitions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RouteCoordinatorKey, Integer> coordinators = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RouteTopicIdKey, String> topicNames = new ConcurrentHashMap<>();

    /**
     * Partition identity (topic name + partition index).
     */
    record PartitionKey(String topicName, int partitionIndex) {}

    /**
     * Cached partition data with the route it was learned from.
     */
    record PartitionEntry(String route, PartitionInfo info) {}

    /**
     * Partition topology: leader, replicas, and in-sync replicas.
     * All node IDs are virtual (already translated by
     * {@link NodeIdResponseTranslator}).
     */
    public record PartitionInfo(int leader, List<Integer> replicas, List<Integer> isr) {
        public PartitionInfo {
            replicas = List.copyOf(replicas);
            isr = List.copyOf(isr);
        }
    }

    /**
     * Broker metadata: host, port, and optional rack.
     */
    public record BrokerInfo(String host, int port, @Nullable String rack) {}

    /**
     * Coordinator cache key: route-scoped by (route, keyType, key).
     *
     * @param route the route name
     * @param keyType 0 for group, 1 for transaction
     * @param key the group or transaction ID
     */
    public record RouteCoordinatorKey(String route, byte keyType, String key) {}

    /**
     * Topic name cache key: route-scoped by (route, topicId).
     * Per-route filter chains can transform topic names differently,
     * so the same cluster-level topic ID can map to different names
     * on different routes.
     *
     * @param route the route name
     * @param topicId the topic UUID
     */
    record RouteTopicIdKey(String route, Uuid topicId) {}

    /**
     * Updates the cache from a translated METADATA response.
     * The response must have already been through
     * {@link NodeIdResponseTranslator} so that all node IDs are virtual.
     *
     * <p><b>Additive semantics:</b> this method adds or overwrites
     * entries for topics present in the response but does not remove
     * entries for topics absent from the response. This means
     * ACL-filtered METADATA responses (which omit unauthorized topics)
     * do not corrupt the cache — they simply contribute a subset of
     * entries.</p>
     *
     * @param route the route this response came from
     * @param data the METADATA response data
     */
    public void updateFromMetadata(String route, MetadataResponseData data) {
        Objects.requireNonNull(route);
        Objects.requireNonNull(data);

        for (var broker : data.brokers()) {
            brokers.put(broker.nodeId(), new BrokerInfo(broker.host(), broker.port(), broker.rack()));
        }

        for (var topic : data.topics()) {
            if (topic.errorCode() != Errors.NONE.code()) {
                continue;
            }
            if (topic.topicId() != null && !Uuid.ZERO_UUID.equals(topic.topicId())
                    && topic.name() != null && !topic.name().isEmpty()) {
                topicNames.put(new RouteTopicIdKey(route, topic.topicId()), topic.name());
            }
            for (var partition : topic.partitions()) {
                if (partition.errorCode() != Errors.NONE.code() || partition.leaderId() < 0) {
                    continue;
                }
                var key = new PartitionKey(topic.name(), partition.partitionIndex());
                var info = new PartitionInfo(
                        partition.leaderId(),
                        partition.replicaNodes(),
                        partition.isrNodes());
                partitions.put(key, new PartitionEntry(route, info));
            }
        }
    }

    /**
     * Caches a coordinator node ID explicitly.
     *
     * @param route the route this coordinator belongs to
     * @param keyType 0 for group, 1 for transaction
     * @param key the group or transaction ID
     * @param nodeId the coordinator's virtual node ID
     */
    public void putCoordinator(String route, byte keyType, String key, int nodeId) {
        coordinators.put(new RouteCoordinatorKey(route, keyType, key), nodeId);
    }

    /**
     * Updates the cache from a translated FIND_COORDINATOR response,
     * using the key type from the original request.
     *
     * <p>The response itself lacks {@code keyType} (and pre-v4
     * responses lack {@code key}), so the key type must be supplied
     * from the request context. For v0-3 (single-coordinator), the
     * key is also taken from the request. For v4+ (batched), the
     * response carries each coordinator's key.</p>
     *
     * @param route the route this response came from
     * @param data the FIND_COORDINATOR response data
     * @param apiVersion the API version of the response
     * @param requestKeyType the key type from the original request
     * @param requestKey the key from the original request (used for v0-3)
     */
    public void updateFromFindCoordinator(String route,
                                          FindCoordinatorResponseData data,
                                          short apiVersion,
                                          byte requestKeyType,
                                          String requestKey) {
        Objects.requireNonNull(route);
        Objects.requireNonNull(data);

        if (apiVersion >= 4) {
            for (var coordinator : data.coordinators()) {
                if (coordinator.errorCode() == Errors.NONE.code() && coordinator.nodeId() >= 0) {
                    coordinators.put(
                            new RouteCoordinatorKey(route, requestKeyType, coordinator.key()),
                            coordinator.nodeId());
                }
            }
        }
        else {
            if (data.errorCode() == Errors.NONE.code() && data.nodeId() >= 0) {
                coordinators.put(
                        new RouteCoordinatorKey(route, requestKeyType, requestKey),
                        data.nodeId());
            }
        }
    }

    /**
     * Returns the full partition info for a topic-partition, or null if not cached.
     */
    @Nullable
    public PartitionInfo partitionInfoFor(String topicName, int partitionIndex) {
        var entry = partitions.get(new PartitionKey(topicName, partitionIndex));
        return entry != null ? entry.info() : null;
    }

    /**
     * Returns the cached leader for a topic-partition, or null if not cached.
     */
    @Nullable
    public Integer leaderFor(String topicName, int partitionIndex) {
        var info = partitionInfoFor(topicName, partitionIndex);
        return info != null ? info.leader() : null;
    }

    /**
     * Returns broker metadata for a virtual node ID, or null if not cached.
     */
    @Nullable
    public BrokerInfo brokerInfo(int virtualNodeId) {
        return brokers.get(virtualNodeId);
    }

    /**
     * Returns the cached coordinator node ID, or null if not cached.
     */
    @Nullable
    public Integer coordinatorFor(String route, byte keyType, String key) {
        return coordinators.get(new RouteCoordinatorKey(route, keyType, key));
    }

    /**
     * Returns the cached topic name for a topic ID on a given route,
     * or null if not cached.
     */
    @Nullable
    public String topicNameFor(String route, Uuid topicId) {
        return topicNames.get(new RouteTopicIdKey(route, topicId));
    }

    /**
     * Returns the subset of the given topic IDs that are not cached
     * for the given route.
     */
    public Set<Uuid> uncachedTopicIds(String route, Set<Uuid> topicIds) {
        return topicIds.stream()
                .filter(id -> !topicNames.containsKey(new RouteTopicIdKey(route, id)))
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Coarse invalidation: clears all partition info, coordinators,
     * broker info, and topic ID to name mappings associated with the
     * given route.
     */
    public void invalidateRoute(String route) {
        Objects.requireNonNull(route);

        partitions.entrySet().removeIf(e -> route.equals(e.getValue().route()));

        coordinators.keySet().removeIf(k -> route.equals(k.route()));

        topicNames.keySet().removeIf(k -> route.equals(k.route()));

        // Broker info doesn't track which route it came from —
        // in multi-route topologies, a broker may appear in responses
        // from multiple routes. Clearing all brokers on any route
        // invalidation is safe (over-invalidation), and they are
        // repopulated from the next METADATA response.
        // Only clear if the invalidated route actually had cached data.
        if (!brokers.isEmpty()) {
            brokers.clear();
        }
    }
}
