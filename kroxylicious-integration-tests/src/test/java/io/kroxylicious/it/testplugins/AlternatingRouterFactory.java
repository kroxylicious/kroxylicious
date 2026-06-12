/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ApiVersionsResponseData;
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
import io.kroxylicious.proxy.router.RouterResult;

/**
 * A router that alternates PRODUCE requests between two routes in
 * configurable batch sizes. All other API keys are statically routed
 * to {@code routeA}. API_VERSIONS responses are intercepted to cap
 * the PRODUCE version below the threshold where topic IDs replace
 * topic names, since the two backing clusters have independent topic IDs.
 */
@Plugin(configType = AlternatingRouterFactory.Config.class)
public class AlternatingRouterFactory implements RouterFactory<AlternatingRouterFactory.Config, AlternatingRouterFactory.Config> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlternatingRouterFactory.class);

    // PRODUCE v13 replaces topic names with topic IDs (KIP-516).
    // When routing between independent clusters the IDs differ, so
    // we cap at v12 to keep name-based addressing.
    private static final short MAX_PRODUCE_VERSION = 12;

    private static final Set<ApiKeys> DYNAMICALLY_ROUTED = Set.of(ApiKeys.PRODUCE, ApiKeys.API_VERSIONS);

    public record Config(String routeA,
                         String routeB,
                         int batchSize) {}

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        String routeA = config.routeA();
        String routeB = config.routeB();
        int batchSize = config.batchSize();
        AtomicInteger counter = new AtomicInteger();

        LOGGER.atInfo()
                .addKeyValue("routeA", routeA)
                .addKeyValue("routeB", routeB)
                .addKeyValue("batchSize", batchSize)
                .log("AlternatingRouter created");

        Map<ApiKeys, String> staticMap = Arrays.stream(ApiKeys.values())
                .filter(k -> !DYNAMICALLY_ROUTED.contains(k))
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> routeA));

        return new Router() {
            @Override
            public CompletionStage<RouterResult> onRequest(
                                                           short apiVersion,
                                                           ApiKeys apiKey,
                                                           RequestHeaderData header,
                                                           ApiMessage request,
                                                           RouterContext routerContext) {
                if (apiKey == ApiKeys.API_VERSIONS) {
                    int nodeId = routerContext.anyNodeId(routeA);
                    return routerContext.sendRequestToNode(nodeId, header, request)
                            .thenApply(response -> {
                                capProduceVersion(response.body());
                                LOGGER.atDebug()
                                        .addKeyValue("sessionId", routerContext.sessionId())
                                        .addKeyValue("cappedMaxVersion", MAX_PRODUCE_VERSION)
                                        .log("Capped PRODUCE version in API_VERSIONS response");
                                return new RouterResult.Completed(response);
                            });
                }

                int index = counter.getAndIncrement();
                String route = ((index / batchSize) % 2 == 0) ? routeA : routeB;
                LOGGER.atDebug()
                        .addKeyValue("sessionId", routerContext.sessionId())
                        .addKeyValue("route", route)
                        .addKeyValue("batchIndex", index)
                        .addKeyValue("batchSize", batchSize)
                        .log("Alternating router chose route based on batch index");
                int nodeId = routerContext.anyNodeId(route);
                return routerContext.sendRequestToNode(nodeId, header, request)
                        .thenApply(RouterResult.Completed::new);
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return staticMap;
            }
        };
    }

    private static void capProduceVersion(ApiMessage responseBody) {
        if (responseBody instanceof ApiVersionsResponseData data) {
            for (var key : data.apiKeys()) {
                if (key.apiKey() == ApiKeys.PRODUCE.id && key.maxVersion() > MAX_PRODUCE_VERSION) {
                    key.setMaxVersion(MAX_PRODUCE_VERSION);
                }
            }
        }
    }
}
