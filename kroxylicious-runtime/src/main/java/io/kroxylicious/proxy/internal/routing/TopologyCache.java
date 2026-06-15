/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.Uuid;
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
 */
public class TopologyCache {

    private final ConcurrentHashMap<PartitionKey, PartitionEntry> partitions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RouteCoordinatorKey, Integer> coordinators = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Uuid, String> topicNames = new ConcurrentHashMap<>();

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
     * Updates the cache from a translated METADATA response.
     * The response must have already been through
     * {@link NodeIdResponseTranslator} so that all node IDs are virtual.
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
                topicNames.put(topic.topicId(), topic.name());
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
     * Caches a coordinator node ID discovered via FIND_COORDINATOR.
     * Called by the TopologyService (not by RoutingDecisionHandler),
     * because the key type and key come from the request context,
     * not from the response (the response lacks keyType, and v0-3
     * responses lack the key entirely).
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
     * Returns the cached topic name for a topic ID, or null if not cached.
     */
    @Nullable
    public String topicNameFor(Uuid topicId) {
        return topicNames.get(topicId);
    }

    /**
     * Coarse invalidation: clears all partition info, coordinators, and
     * broker info associated with the given route. Topic ID to name
     * mappings are not cleared (they are stable within a cluster).
     */
    public void invalidateRoute(String route) {
        Objects.requireNonNull(route);

        partitions.entrySet().removeIf(e -> route.equals(e.getValue().route()));

        coordinators.keySet().removeIf(k -> route.equals(k.route()));

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
