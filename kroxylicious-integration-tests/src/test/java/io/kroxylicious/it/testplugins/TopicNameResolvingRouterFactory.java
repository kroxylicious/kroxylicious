/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.topology.TopologyService;
import io.kroxylicious.proxy.topology.VirtualNode;

/**
 * A test router that resolves topic IDs via the topology service and captures
 * the per-route results for assertion. Uses {@code allowSharedClusterTargets()}
 * so that multiple routes can target the same cluster.
 */
@Plugin(configType = TopicNameResolvingRouterFactory.Config.class)
public class TopicNameResolvingRouterFactory
        implements RouterFactory<TopicNameResolvingRouterFactory.Config, TopicNameResolvingRouterFactory.InitData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicNameResolvingRouterFactory.class);

    private static final ConcurrentLinkedQueue<ResolvedNames> CAPTURES = new ConcurrentLinkedQueue<>();

    public record Config(String defaultRoute, List<String> allRoutes) {}

    public record InitData(Config config) {}

    /**
     * A captured topic name resolution result: route → {topicId → name}.
     */
    public record ResolvedNames(Map<String, Map<Uuid, String>> byRoute) {}

    public static List<ResolvedNames> drainCaptures() {
        var result = List.copyOf(CAPTURES);
        CAPTURES.clear();
        return result;
    }

    public static void reset() {
        CAPTURES.clear();
    }

    @Override
    public InitData initialize(RouterFactoryContext context, Config config) {
        context.allowSharedClusterTargets();
        context.topologyService();
        return new InitData(config);
    }

    @Override
    public Router createRouter(RouterFactoryContext context, InitData initData) {
        Config config = initData.config();
        TopologyService topologyService = context.topologyService();

        Set<ApiKeys> dynamicallyRouted = Set.of(ApiKeys.PRODUCE, ApiKeys.FETCH, ApiKeys.METADATA);
        Map<ApiKeys, String> staticMap = Arrays.stream(ApiKeys.values())
                .filter(k -> !dynamicallyRouted.contains(k))
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> config.defaultRoute()));

        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(
                                                             ApiKeys apiKey, short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext routerContext) {

                if (apiKey == ApiKeys.METADATA) {
                    return handleMetadata(header, request, routerContext);
                }

                Set<Uuid> topicIds = TopicIdExtractor.extractTopicIds(apiKey, request);

                if (topicIds.isEmpty()) {
                    VirtualNode node = routerContext.anyNode(config.defaultRoute());
                    return routerContext.sendRequest(node, header, request)
                            .thenApply(response -> routerContext.respondWith(response).build());
                }

                Map<String, Map<Uuid, String>> byRoute = new HashMap<>();
                CompletableFuture<Void> combined = CompletableFuture.completedFuture(null);
                for (String route : config.allRoutes()) {
                    combined = combined.thenCombine(
                            topologyService.topicNames(route, topicIds),
                            (v, names) -> {
                                byRoute.put(route, new HashMap<>(names));
                                return null;
                            });
                }

                return combined.thenCompose(v -> {
                    CAPTURES.add(new ResolvedNames(byRoute));
                    LOGGER.atDebug()
                            .addKeyValue("resolvedNames", byRoute)
                            .log("Captured topic name resolutions");

                    VirtualNode node = routerContext.anyNode(config.defaultRoute());
                    return routerContext.sendRequest(node, header, request)
                            .thenApply(response -> routerContext.respondWith(response).build());
                });
            }

            private CompletionStage<RouterResponse> handleMetadata(
                                                                   RequestHeaderData header,
                                                                   ApiMessage request,
                                                                   RouterContext routerContext) {
                CompletableFuture<ApiMessage> primaryResponse = new CompletableFuture<>();
                CompletableFuture<Void> allDone = CompletableFuture.completedFuture(null);
                for (String route : config.allRoutes()) {
                    VirtualNode node = routerContext.anyNode(route);
                    CompletionStage<ApiMessage> future = routerContext.sendRequest(node, header, request);
                    if (route.equals(config.defaultRoute())) {
                        allDone = allDone.thenCombine(future, (v, resp) -> {
                            primaryResponse.complete(resp);
                            return null;
                        });
                    }
                    else {
                        allDone = allDone.thenCombine(future, (v, resp) -> null);
                    }
                }
                return allDone.thenApply(v -> routerContext.respondWith(primaryResponse.join()).build());
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return staticMap;
            }
        };
    }
}
