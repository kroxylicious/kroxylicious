/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.proxy.topology.BrokerInfo;

/**
 * Thread-safe cache of cluster topology data, populated from METADATA (and, in later phases,
 * FIND_COORDINATOR) responses. Shared per router level, not per connection - see
 * {@link io.kroxylicious.proxy.topology.TopologyService}'s "cache scope" documentation for why.
 *
 * <p>Only the topic-id-to-name mapping and broker info are implemented so far (phases 1-2 of
 * <a href="https://github.com/kroxylicious/kroxylicious/issues/4155">#4155</a>). Later phases add
 * further maps here (partition leaders/replicas/ISR, coordinators) and extend
 * {@link #invalidateRoute} to clear them too.
 *
 * <p><b>Additive semantics:</b> {@link #updateFromMetadata} adds or overwrites entries present in
 * a response but never removes entries absent from it. This means ACL-filtered METADATA responses
 * (which omit unauthorized topics) do not corrupt the cache - they simply contribute a subset of
 * entries. See {@link io.kroxylicious.proxy.topology.TopologyService} for why this is safe even
 * though the cache is not scoped by authenticated subject.
 */
public final class TopologyCache {

    private final ConcurrentHashMap<String, ConcurrentHashMap<Uuid, String>> topicNamesByRoute = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, BrokerInfo>> brokersByRoute = new ConcurrentHashMap<>();

    /**
     * Creates an empty cache.
     */
    TopologyCache() {
        // Exists for Javadoc
    }

    /**
     * Updates the cache from a METADATA response received on the given route.
     *
     * @param route the route this response came from
     * @param response the METADATA response data
     */
    void updateFromMetadata(String route, MetadataResponseData response) {
        Objects.requireNonNull(route);
        Objects.requireNonNull(response);
        if (response.topics() != null) {
            for (var topic : response.topics()) {
                if (isCacheable(topic)) {
                    topicNamesByRoute.computeIfAbsent(route, r -> new ConcurrentHashMap<>())
                            .put(topic.topicId(), topic.name());
                }
            }
        }
        if (response.brokers() != null) {
            for (var broker : response.brokers()) {
                brokersByRoute.computeIfAbsent(route, r -> new ConcurrentHashMap<>())
                        .put(broker.nodeId(), new BrokerInfo(broker.host(), broker.port(), broker.rack()));
            }
        }
    }

    private boolean isCacheable(MetadataResponseData.MetadataResponseTopic topic) {
        return topic.name() != null && !topic.name().isEmpty()
                && topic.topicId() != null && !Uuid.ZERO_UUID.equals(topic.topicId());
    }

    /**
     * Returns the cached name for a topic ID on the given route, or empty if not cached.
     *
     * @param route the route the topic ID was learned on
     * @param topicId the topic ID to resolve
     * @return the cached topic name, or empty if not cached
     */
    Optional<String> topicName(String route, Uuid topicId) {
        var names = topicNamesByRoute.get(route);
        return names == null ? Optional.empty() : Optional.ofNullable(names.get(topicId));
    }

    /**
     * Returns the cached broker info for a virtual node ID on the given route, or empty if not
     * cached.
     *
     * @param route the route the broker was learned on
     * @param virtualNodeId the virtual node ID to resolve
     * @return the cached broker info, or empty if not cached
     */
    Optional<BrokerInfo> brokerInfo(String route, int virtualNodeId) {
        var brokers = brokersByRoute.get(route);
        return brokers == null ? Optional.empty() : Optional.ofNullable(brokers.get(virtualNodeId));
    }

    /**
     * Coarse invalidation: clears all cached data for the given route.
     *
     * @param route the route to invalidate
     */
    void invalidateRoute(String route) {
        Objects.requireNonNull(route);
        topicNamesByRoute.remove(route);
        brokersByRoute.remove(route);
    }
}
