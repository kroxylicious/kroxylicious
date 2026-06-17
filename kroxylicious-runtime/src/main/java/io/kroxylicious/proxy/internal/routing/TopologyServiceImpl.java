/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.topology.BrokerInfo;
import io.kroxylicious.proxy.topology.Coordinators;
import io.kroxylicious.proxy.topology.PartitionInfo;
import io.kroxylicious.proxy.topology.PartitionLeaders;
import io.kroxylicious.proxy.topology.TopologyService;
import io.kroxylicious.proxy.topology.VirtualNode;

/**
 * Per-connection implementation of {@link TopologyService}, backed
 * by a shared {@link TopologyCache}.
 *
 * <p>One instance is created per client connection in
 * {@link io.kroxylicious.proxy.bootstrap.RouterChainFactory#createRouter}.
 * All access happens on the single Netty event loop thread assigned
 * to that connection — no synchronisation is needed.</p>
 *
 * <p>Discovery methods ({@link #leaders}, {@link #coordinators},
 * {@link #topicNames}) use a {@link RequestSender} bound
 * per-connection via {@link #bindRequestSender}. The cache is
 * populated as a side effect of responses flowing through
 * {@link RoutingDecisionHandler#write} — by the time the
 * {@code CompletionStage} returned by the sender completes,
 * the cache is guaranteed to be updated.</p>
 */
public class TopologyServiceImpl implements TopologyService {

    private static final short INTERNAL_METADATA_API_VERSION = 12;
    private static final short FIND_COORDINATOR_API_VERSION = 3;

    private final TopologyCache cache;
    private RequestSender requestSender;

    /**
     * Sends a request to a route and returns a stage that completes
     * with the response.
     */
    @FunctionalInterface
    public interface RequestSender {
        CompletionStage<ApiMessage> send(String route, RequestHeaderData header, ApiMessage request);
    }

    public TopologyServiceImpl(TopologyCache cache) {
        this.cache = Objects.requireNonNull(cache);
    }

    TopologyCache cache() {
        return cache;
    }

    /**
     * Binds the request-sending capability for this connection.
     * Called before each {@link RoutingDecisionHandler#channelRead}
     * dispatch on the connection's event loop thread.
     */
    public void bindRequestSender(RequestSender sender) {
        this.requestSender = Objects.requireNonNull(sender);
    }

    private RequestSender requireSender() {
        RequestSender s = requestSender;
        if (s == null) {
            throw new IllegalStateException(
                    "No RequestSender bound — topology requests can only be sent during request processing");
        }
        return s;
    }

    // --- Discovery methods ---

    @Override
    public CompletionStage<PartitionLeaders> leaders(Map<String, Set<String>> topicsByRoute) {
        RequestSender sender = requireSender();
        List<CompletionStage<Void>> fetches = new ArrayList<>();

        for (var entry : topicsByRoute.entrySet()) {
            String route = entry.getKey();
            Set<String> topics = entry.getValue();

            Set<String> uncached = topics.stream()
                    .filter(topic -> cache.leaderFor(topic, 0) == null)
                    .collect(Collectors.toSet());

            if (uncached.isEmpty()) {
                continue;
            }

            var mdHeader = new RequestHeaderData()
                    .setRequestApiKey(ApiKeys.METADATA.id)
                    .setRequestApiVersion(INTERNAL_METADATA_API_VERSION);
            var mdReq = new MetadataRequestData();
            for (var name : uncached) {
                mdReq.topics().add(new MetadataRequestData.MetadataRequestTopic().setName(name));
            }

            fetches.add(sender.send(route, mdHeader, mdReq).thenAccept(resp -> {
            }));
        }

        if (fetches.isEmpty()) {
            return CompletableFuture.completedFuture(snapshotLeaders(topicsByRoute));
        }
        CompletableFuture<Void> combined = CompletableFuture.completedFuture(null);
        for (var fetch : fetches) {
            combined = combined.thenCombine(fetch, (a, b) -> null);
        }
        return combined.thenApply(v -> snapshotLeaders(topicsByRoute));
    }

    private PartitionLeaders snapshotLeaders(Map<String, Set<String>> topicsByRoute) {
        return (topicName, partitionIndex) -> {
            Integer leader = cache.leaderFor(topicName, partitionIndex);
            return leader != null ? Optional.of(new VirtualNodeImpl(leader)) : Optional.empty();
        };
    }

    @Override
    public CompletionStage<Coordinators> coordinators(String route, byte keyType, Set<String> keys) {
        RequestSender sender = requireSender();

        var mdHeader = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.METADATA.id)
                .setRequestApiVersion(INTERNAL_METADATA_API_VERSION);
        var mdReq = new MetadataRequestData();

        return sender.send(route, mdHeader, mdReq).thenCompose(mdResponse -> {
            var fcHeader = new RequestHeaderData()
                    .setRequestApiKey(ApiKeys.FIND_COORDINATOR.id)
                    .setRequestApiVersion(FIND_COORDINATOR_API_VERSION);
            var fcReq = new FindCoordinatorRequestData()
                    .setKeyType(keyType);

            if (keys.size() == 1) {
                fcReq.setKey(keys.iterator().next());
            }
            else {
                for (var key : keys) {
                    fcReq.coordinatorKeys().add(key);
                }
            }

            return sender.send(route, fcHeader, fcReq);
        }).thenApply(coordResponse -> {
            var resp = (FindCoordinatorResponseData) coordResponse;
            if (resp.errorCode() != Errors.NONE.code()) {
                throw new CoordinatorDiscoveryException(Errors.forCode(resp.errorCode()));
            }
            return snapshotCoordinators(route, keyType, keys);
        });
    }

    private Coordinators snapshotCoordinators(String route, byte keyType, Set<String> keys) {
        return key -> {
            Integer nodeId = cache.coordinatorFor(route, keyType, key);
            return nodeId != null ? Optional.of(new VirtualNodeImpl(nodeId)) : Optional.empty();
        };
    }

    @Override
    public CompletionStage<Map<Uuid, String>> topicNames(Set<Uuid> topicIds) {
        return CompletableFuture.completedFuture(resolveFromCache(topicIds));
    }

    private Map<Uuid, String> resolveFromCache(Set<Uuid> topicIds) {
        var result = new HashMap<Uuid, String>();
        for (var id : topicIds) {
            String name = cache.topicNameFor(id);
            if (name != null) {
                result.put(id, name);
            }
        }
        return result;
    }

    // --- Supplementary lookups ---

    @Override
    public Optional<PartitionInfo> partitionInfo(String topicName, int partitionIndex) {
        TopologyCache.PartitionInfo info = cache.partitionInfoFor(topicName, partitionIndex);
        if (info == null) {
            return Optional.empty();
        }
        return Optional.of(new PartitionInfo(
                new VirtualNodeImpl(info.leader()),
                info.replicas().stream().<VirtualNode> map(VirtualNodeImpl::new).toList(),
                info.isr().stream().<VirtualNode> map(VirtualNodeImpl::new).toList()));
    }

    @Override
    public Optional<BrokerInfo> brokerInfo(VirtualNode node) {
        int nodeId = ((VirtualNodeImpl) node).encodedId();
        TopologyCache.BrokerInfo info = cache.brokerInfo(nodeId);
        if (info == null) {
            return Optional.empty();
        }
        return Optional.of(new BrokerInfo(info.host(), info.port(), info.rack()));
    }

    @Override
    public void invalidateRoute(String route) {
        cache.invalidateRoute(route);
    }
}
