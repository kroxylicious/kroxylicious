/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins.router;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.RouterResponse;

/**
 * A test router factory that forwards everything to a single named route but routes
 * PRODUCE requests dynamically (via {@code onRequest}) rather than statically.
 * Used by integration tests to verify that the dynamic routing path handles
 * real Kafka requests end-to-end.
 */
@Plugin(configType = DynamicProduceRouterFactory.Config.class)
public class DynamicProduceRouterFactory
        implements RouterFactory<DynamicProduceRouterFactory.Config, DynamicProduceRouterFactory.Config> {

    public record Config(String route) {}

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        String route = config.route();
        Map<ApiKeys, String> staticMap = Arrays.stream(ApiKeys.values())
                .filter(k -> k != ApiKeys.PRODUCE)
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> route));

        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey,
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext ctx) {
                if (request instanceof ProduceRequestData pd && pd.acks() == 0) {
                    return ctx.sendToRoute(route, header, request)
                            .thenCompose(ignored -> ctx.respondWithoutReply().completed());
                }
                return ctx.sendToRoute(route, header, request)
                        .thenCompose(body -> ctx.respondWith(body).completed());
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return staticMap;
            }
        };
    }
}
