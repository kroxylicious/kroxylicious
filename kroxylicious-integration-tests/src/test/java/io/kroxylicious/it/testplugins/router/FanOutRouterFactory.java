/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins.router;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

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
 * A test router that fans a single dynamically-routed API key out to every configured route,
 * sending the request down each route concurrently, and delivers the first route's response back
 * to the client. All other API keys are statically routed to the first route.
 *
 * <p>Used to exercise the case where two or more routes issue an out-of-band request on the same
 * client connection at once (see {@code RouteFilterCorrectnessIT}).</p>
 */
@Plugin(configType = FanOutRouterFactory.Config.class)
public class FanOutRouterFactory implements RouterFactory<FanOutRouterFactory.Config, FanOutRouterFactory.Config> {

    /**
     * Configuration for {@link FanOutRouterFactory}.
     *
     * @param routes        the routes to fan the dynamic API key out to, in order; the first route
     *                      also statically handles every other API key
     * @param dynamicApiKey name of the {@link ApiKeys} constant that is fanned out
     */
    public record Config(List<String> routes, String dynamicApiKey) {}

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        ApiKeys dynamic = ApiKeys.valueOf(config.dynamicApiKey());
        String defaultRoute = config.routes().get(0);
        Map<ApiKeys, String> staticMap = Arrays.stream(ApiKeys.values())
                .filter(k -> k != dynamic)
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> defaultRoute));
        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey,
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext ctx) {
                // Send the request down every route first, so all of them are in flight (and any
                // per-route filter's out-of-band request is issued) before any response is awaited.
                List<CompletableFuture<ApiMessage>> stages = config.routes().stream()
                        .map(route -> ctx.sendRequest(ctx.anyNode(route), header, request).toCompletableFuture())
                        .toList();
                return CompletableFuture.allOf(stages.toArray(new CompletableFuture[0]))
                        .thenCompose(ignored -> ctx.respondWith(stages.getFirst().getNow(null)).completed());
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return staticMap;
            }
        };
    }
}
