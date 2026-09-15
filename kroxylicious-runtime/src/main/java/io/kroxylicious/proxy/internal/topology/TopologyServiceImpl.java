/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.topology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.proxy.topology.BrokerInfo;
import io.kroxylicious.proxy.topology.Coordinators;
import io.kroxylicious.proxy.topology.PartitionInfo;
import io.kroxylicious.proxy.topology.PartitionLeaders;
import io.kroxylicious.proxy.topology.TopologyService;
import io.kroxylicious.proxy.topology.VirtualNode;

/**
 * Per-connection implementation of {@link TopologyService}, backed by a {@link TopologyCache}
 * shared across all connections at the same router level.
 *
 * <p>Only {@link #topicNames} and {@link #invalidateRoute} are implemented so far (phase 1 of
 * <a href="https://github.com/kroxylicious/kroxylicious/issues/4155">#4155</a>); the remaining
 * discovery/lookup methods throw {@link UnsupportedOperationException} until later phases fill
 * them in.
 */
public final class TopologyServiceImpl implements TopologyService {

    /** The first METADATA API version that supports requesting topics by topic id. */
    private static final short METADATA_API_VER_WITH_TOPIC_ID_SUPPORT = 12;
    private static final String NOT_YET_IMPLEMENTED = " is not yet implemented, see https://github.com/kroxylicious/kroxylicious/issues/4155";

    private final TopologyCache cache;
    private final RequestSender sender;

    /**
     * Creates a per-connection {@link TopologyService} backed by the given shared cache.
     *
     * @param cache the shared topology cache for this router level
     * @param sender the request-sending capability for this connection
     */
    public TopologyServiceImpl(TopologyCache cache, RequestSender sender) {
        this.cache = Objects.requireNonNull(cache);
        this.sender = Objects.requireNonNull(sender);
    }

    @Override
    public CompletionStage<Map<Uuid, String>> topicNames(String route, Set<Uuid> topicIds) {
        Objects.requireNonNull(route);
        Objects.requireNonNull(topicIds);
        Map<Uuid, String> resolved = new HashMap<>();
        Set<Uuid> missing = new HashSet<>();
        for (var id : topicIds) {
            cache.topicName(route, id).ifPresentOrElse(
                    name -> resolved.put(id, name),
                    () -> missing.add(id));
        }
        if (missing.isEmpty()) {
            return CompletableFuture.completedFuture(Map.copyOf(resolved));
        }

        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.METADATA.id)
                .setRequestApiVersion(METADATA_API_VER_WITH_TOPIC_ID_SUPPORT);
        var request = new MetadataRequestData()
                .setAllowAutoTopicCreation(false)
                .setTopics(missing.stream().map(id -> new MetadataRequestData.MetadataRequestTopic().setTopicId(id)).toList());

        // RouteDispatcher.handleResponse() populates the cache from this response as a side
        // effect before this stage completes (see TopologyService's "cache population"
        // documentation), so re-reading the cache here is sufficient - no need to parse the
        // response body directly.
        return sender.sendToAnyNode(route, header, request).thenApply(ignored -> {
            for (var id : missing) {
                cache.topicName(route, id).ifPresent(name -> resolved.put(id, name));
            }
            return Map.copyOf(resolved);
        });
    }

    @Override
    public void invalidateRoute(String route) {
        cache.invalidateRoute(Objects.requireNonNull(route));
    }

    @Override
    public CompletionStage<PartitionLeaders> leaders(Map<String, Set<String>> topicsByRoute) {
        throw new UnsupportedOperationException("TopologyService.leaders()" + NOT_YET_IMPLEMENTED);
    }

    @Override
    public CompletionStage<Coordinators> coordinators(String route, byte keyType, Set<String> keys) {
        throw new UnsupportedOperationException("TopologyService.coordinators()" + NOT_YET_IMPLEMENTED);
    }

    @Override
    public Optional<PartitionInfo> partitionInfo(String topicName, int partitionIndex) {
        throw new UnsupportedOperationException("TopologyService.partitionInfo()" + NOT_YET_IMPLEMENTED);
    }

    @Override
    public Optional<BrokerInfo> brokerInfo(VirtualNode node) {
        throw new UnsupportedOperationException("TopologyService.brokerInfo()" + NOT_YET_IMPLEMENTED);
    }
}
