/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.topology;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.message.MetadataResponseData;

/**
 * Thread-safe cache of cluster topology data, populated from METADATA (and, in later phases,
 * FIND_COORDINATOR) responses. Shared per router level, not per connection - see
 * {@link io.kroxylicious.proxy.topology.TopologyService}'s "cache scope" documentation for why.
 *
 * <p>Only the topic-id-to-name mapping is implemented so far (phase 1 of
 * <a href="https://github.com/kroxylicious/kroxylicious/issues/4155">#4155</a>). Later phases add
 * further maps here (partition leaders/replicas/ISR, coordinators, broker info) and extend
 * {@link #invalidateRoute} to clear them too.
 *
 * <p><b>Additive semantics:</b> {@link #updateFromMetadata} adds or overwrites entries for topics
 * present in a response but never removes entries for topics absent from it. This means
 * ACL-filtered METADATA responses (which omit unauthorized topics) do not corrupt the cache -
 * they simply contribute a subset of entries. See {@link io.kroxylicious.proxy.topology.TopologyService}
 * for why this is safe even though the cache is not scoped by authenticated subject.
 */
public final class TopologyCache {

    private final ConcurrentHashMap<String, ConcurrentHashMap<Uuid, String>> topicNamesByRoute = new ConcurrentHashMap<>();

    /**
     * Creates an empty cache.
     */
    public TopologyCache() {
    }

    /**
     * Updates the cache from a METADATA response received on the given route.
     *
     * @param route the route this response came from
     * @param response the METADATA response data
     */
    public void updateFromMetadata(String route, MetadataResponseData response) {
        Objects.requireNonNull(route);
        Objects.requireNonNull(response);
        if (response.topics() == null) {
            return;
        }
        for (var topic : response.topics()) {
            if (topic.name() != null && !topic.name().isEmpty()
                    && topic.topicId() != null && !Uuid.ZERO_UUID.equals(topic.topicId())) {
                topicNamesByRoute.computeIfAbsent(route, r -> new ConcurrentHashMap<>())
                        .put(topic.topicId(), topic.name());
            }
        }
    }

    /**
     * Returns the cached name for a topic ID on the given route, or empty if not cached.
     *
     * @param route the route the topic ID was learned on
     * @param topicId the topic ID to resolve
     * @return the cached topic name, or empty if not cached
     */
    public Optional<String> topicName(String route, Uuid topicId) {
        var names = topicNamesByRoute.get(route);
        return names == null ? Optional.empty() : Optional.ofNullable(names.get(topicId));
    }

    /**
     * Coarse invalidation: clears all cached data for the given route.
     *
     * @param route the route to invalidate
     */
    public void invalidateRoute(String route) {
        Objects.requireNonNull(route);
        topicNamesByRoute.remove(route);
    }
}
