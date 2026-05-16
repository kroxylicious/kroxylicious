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

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.routing.Router;
import io.kroxylicious.proxy.routing.RouterFactory;
import io.kroxylicious.proxy.routing.RouterFactoryContext;
import io.kroxylicious.proxy.routing.RoutingContext;
import io.kroxylicious.proxy.routing.RoutingResult;

/**
 * A router that alternates PRODUCE requests between two routes in
 * configurable batch sizes. All other API keys are statically routed
 * to {@code routeA}. API_VERSIONS responses are intercepted to cap
 * the PRODUCE version below the threshold where topic IDs replace
 * topic names, since the two backing clusters have independent topic IDs.
 */
@Plugin(configType = AlternatingRouterFactory.Config.class)
public class AlternatingRouterFactory implements RouterFactory<AlternatingRouterFactory.Config, AlternatingRouterFactory.Config> {

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

        Map<ApiKeys, String> staticMap = Arrays.stream(ApiKeys.values())
                .filter(k -> !DYNAMICALLY_ROUTED.contains(k))
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> routeA));

        return new Router() {
            @Override
            public CompletionStage<RoutingResult> onClientRequest(
                                                                  short apiVersion,
                                                                  ApiKeys apiKey,
                                                                  RequestHeaderData header,
                                                                  ApiMessage request,
                                                                  RoutingContext routingContext) {
                if (apiKey == ApiKeys.API_VERSIONS) {
                    return routingContext.sendRequest(routeA, header, request)
                            .thenApply(response -> {
                                capProduceVersion(response.body());
                                routingContext.sendResponse(response);
                                return RoutingResult.completed();
                            });
                }

                int index = counter.getAndIncrement();
                String route = ((index / batchSize) % 2 == 0) ? routeA : routeB;
                return routingContext.sendRequest(route, header, request)
                        .thenApply(response -> {
                            routingContext.sendResponse(response);
                            return RoutingResult.completed();
                        });
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
