/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins.router;

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
import io.kroxylicious.proxy.router.RouterResponse;

/**
 * A test router that alternates PRODUCE requests between two routes in configurable batch sizes.
 * All other API keys are statically routed to {@code routeA}. API_VERSIONS responses are
 * intercepted to cap the PRODUCE version below the threshold where topic IDs replace topic
 * names, since the two backing clusters have independent topic IDs.
 */
@Plugin(configType = AlternatingRouterFactory.Config.class)
public class AlternatingRouterFactory implements RouterFactory<AlternatingRouterFactory.Config, AlternatingRouterFactory.Config> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlternatingRouterFactory.class);

    // PRODUCE v13 replaces topic names with topic IDs (KIP-516).
    // When routing between independent clusters, the IDs differ, so cap at v12.
    private static final short MAX_PRODUCE_VERSION = 12;

    private static final Set<ApiKeys> DYNAMICALLY_ROUTED = Set.of(ApiKeys.PRODUCE, ApiKeys.API_VERSIONS);

    public record Config(String routeA, String routeB, int batchSize) {}

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
            public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey,
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext ctx) {
                if (apiKey == ApiKeys.API_VERSIONS) {
                    var node = ctx.anyNode(routeA);
                    return ctx.sendRequest(node, header, request)
                            .thenCompose(body -> {
                                capProduceVersion(body);
                                LOGGER.atDebug()
                                        .addKeyValue("sessionId", ctx.sessionId())
                                        .addKeyValue("cappedMaxVersion", MAX_PRODUCE_VERSION)
                                        .log("Capped PRODUCE version in API_VERSIONS response");
                                return ctx.respondWith(body).completed();
                            });
                }

                int index = counter.getAndIncrement();
                String route = ((index / batchSize) % 2 == 0) ? routeA : routeB;
                LOGGER.atDebug()
                        .addKeyValue("sessionId", ctx.sessionId())
                        .addKeyValue("route", route)
                        .addKeyValue("batchIndex", index)
                        .addKeyValue("batchSize", batchSize)
                        .log("Alternating router chose route based on batch index");
                var node = ctx.anyNode(route);
                return ctx.sendRequest(node, header, request)
                        .thenCompose(body -> ctx.respondWith(body).completed());
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
