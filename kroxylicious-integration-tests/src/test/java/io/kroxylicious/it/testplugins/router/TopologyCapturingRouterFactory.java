/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins.router;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.message.DescribeClusterResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.topology.BrokerInfo;
import io.kroxylicious.proxy.topology.TopologyService;

/**
 * A test router factory that statically routes everything to a single named route except
 * DESCRIBE_CLUSTER, which it routes dynamically after first calling
 * {@link TopologyService#topicNames}. Used by integration tests to verify that
 * {@code TopologyService} resolves topic ids via a real METADATA round-trip, that
 * {@link TopologyService#invalidateRoute} forces a fresh one, and that
 * {@link TopologyService#brokerInfo} resolves broker host/port/rack for a node learned from the
 * DESCRIBE_CLUSTER response.
 */
@Plugin(configType = TopologyCapturingRouterFactory.Config.class)
public class TopologyCapturingRouterFactory
        implements RouterFactory<TopologyCapturingRouterFactory.Config, TopologyCapturingRouterFactory.Config> {

    public record Config(String route) {}

    public static final AtomicReference<Set<Uuid>> topicIdsToResolve = new AtomicReference<>(Set.of());
    public static final AtomicReference<Map<Uuid, String>> capturedTopicNames = new AtomicReference<>();
    public static final AtomicReference<Optional<BrokerInfo>> capturedBrokerInfo = new AtomicReference<>();
    public static final AtomicReference<TopologyService> capturedTopologyService = new AtomicReference<>();

    public static void reset() {
        topicIdsToResolve.set(Set.of());
        capturedTopicNames.set(null);
        capturedBrokerInfo.set(null);
        capturedTopologyService.set(null);
    }

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        // Proves the opt-in cache-creation side effect is harmless even though this instance
        // must not be used for discovery - see TopologyService's javadoc.
        context.topologyService();
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        String route = config.route();
        TopologyService topologyService = context.topologyService();
        capturedTopologyService.set(topologyService);
        Map<ApiKeys, String> staticMap = Arrays.stream(ApiKeys.values())
                .filter(k -> k != ApiKeys.DESCRIBE_CLUSTER)
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> route));

        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey,
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext ctx) {
                return topologyService.topicNames(route, topicIdsToResolve.get())
                        .thenCompose(names -> {
                            capturedTopicNames.set(names);
                            var node = ctx.anyNode(route);
                            return ctx.sendRequest(node, header, request)
                                    .thenCompose(body -> {
                                        if (body instanceof DescribeClusterResponseData describeClusterResponse) {
                                            describeClusterResponse.brokers().stream().findFirst()
                                                    .ifPresent(broker -> capturedBrokerInfo.set(
                                                            topologyService.brokerInfo(ctx.nodeForId(broker.brokerId()))));
                                        }
                                        return ctx.respondWith(body).completed();
                                    });
                        });
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return staticMap;
            }
        };
    }
}
