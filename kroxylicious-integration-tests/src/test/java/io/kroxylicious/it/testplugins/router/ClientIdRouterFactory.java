/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins.router;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.topology.VirtualNode;

/**
 * Routes requests based on the Kafka client ID in the first request header seen
 * on a given connection. The resolved route is sticky for the lifetime of the
 * {@link Router} instance (one per connection), matching Kafka's connection-level
 * client-ID semantics. All requests are dynamically routed (no static routes).
 * Used as the inner level in nested routing tests.
 */
@Plugin(configType = ClientIdRouterFactory.Config.class)
public class ClientIdRouterFactory
        implements RouterFactory<ClientIdRouterFactory.Config, ClientIdRouterFactory.Config> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientIdRouterFactory.class);

    public record Config(Map<String, String> clientIdRoutes,
                         String defaultRoute) {}

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        String defaultRoute = config.defaultRoute();
        LOGGER.atInfo()
                .addKeyValue("clientIdRoutes", config.clientIdRoutes())
                .addKeyValue("defaultRoute", defaultRoute)
                .log("ClientIdRouter created");

        return new Router() {
            private String resolvedRoute;

            @Override
            public CompletionStage<RouterResponse> onRequest(
                                                             ApiKeys apiKey, short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext routerContext) {
                String route = resolveRoute(header);
                VirtualNode node = routerContext.anyNode(route);
                return routerContext.sendRequest(node, header, request)
                        .thenApply(response -> routerContext.respondWith(response).build());
            }

            // Sticky: resolved once on the first request and reused for the
            // lifetime of this Router instance (one per connection).
            private String resolveRoute(RequestHeaderData header) {
                if (resolvedRoute == null) {
                    resolvedRoute = config.clientIdRoutes()
                            .getOrDefault(header.clientId(), defaultRoute);
                    LOGGER.atDebug()
                            .addKeyValue("clientId", header.clientId())
                            .addKeyValue("route", resolvedRoute)
                            .log("Resolved route from client ID");
                }
                return resolvedRoute;
            }
        };
    }
}
