/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.Map;
import java.util.concurrent.CompletionStage;

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
 * Routes PRODUCE requests to a route determined by the Kafka client
 * ID from the request header. All other API keys are forwarded to
 * the default route. This router is used as the inner level in
 * nested routing tests.
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
            public CompletionStage<RouterResult> onRequest(
                                                           short apiVersion,
                                                           ApiKeys apiKey,
                                                           RequestHeaderData header,
                                                           ApiMessage request,
                                                           RouterContext routerContext) {
                String route = resolveRoute(header);
                int nodeId = routerContext.anyNodeId(route);
                return routerContext.sendRequestToNode(nodeId, header, request)
                        .thenApply(RouterResult.Completed::new);
            }

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
