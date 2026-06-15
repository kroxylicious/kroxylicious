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
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.router.VirtualNode;

/**
 * Routes PRODUCE requests to a route determined by the authenticated
 * principal name. All other API keys are statically routed to the
 * default route. API_VERSIONS is dynamically intercepted to cap
 * PRODUCE at v12 (avoiding topic-ID-based addressing across
 * independent clusters).
 */
@Plugin(configType = PrincipalRouterFactory.Config.class)
public class PrincipalRouterFactory
        implements RouterFactory<PrincipalRouterFactory.Config, PrincipalRouterFactory.Config> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrincipalRouterFactory.class);
    private static final short MAX_PRODUCE_VERSION = 12;
    private static final Set<ApiKeys> DYNAMICALLY_ROUTED = Set.of(ApiKeys.PRODUCE, ApiKeys.API_VERSIONS);

    public record Config(Map<String, String> principalRoutes,
                         String defaultRoute) {}

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        String defaultRoute = config.defaultRoute();
        LOGGER.atInfo()
                .addKeyValue("principalRoutes", config.principalRoutes())
                .addKeyValue("defaultRoute", defaultRoute)
                .log("PrincipalRouter created");

        Map<ApiKeys, String> staticMap = Arrays.stream(ApiKeys.values())
                .filter(k -> !DYNAMICALLY_ROUTED.contains(k))
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> defaultRoute));

        return new Router() {
            private String resolvedRoute;

            @Override
            public CompletionStage<RouterResponse> onRequest(
                                                             short apiVersion,
                                                             ApiKeys apiKey,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext routerContext) {
                if (apiKey == ApiKeys.API_VERSIONS) {
                    VirtualNode node = routerContext.anyNode(defaultRoute);
                    return routerContext.sendRequest(node, header, request)
                            .thenApply(response -> {
                                capProduceVersion(response);
                                return routerContext.respondWith(response).build();
                            });
                }

                String route = resolveRoute(routerContext);
                VirtualNode node = routerContext.anyNode(route);
                return routerContext.sendRequest(node, header, request)
                        .thenApply(response -> routerContext.respondWith(response).build());
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return staticMap;
            }

            private String resolveRoute(RouterContext routerContext) {
                if (resolvedRoute != null) {
                    return resolvedRoute;
                }
                var subject = routerContext.authenticatedSubject();
                String route = subject.uniquePrincipalOfType(User.class)
                        .map(user -> config.principalRoutes()
                                .getOrDefault(user.name(), defaultRoute))
                        .orElse(defaultRoute);
                if (!subject.isAnonymous()) {
                    resolvedRoute = route;
                    LOGGER.atDebug()
                            .addKeyValue("sessionId", routerContext.sessionId())
                            .addKeyValue("route", route)
                            .log("Resolved route from principal");
                }
                return route;
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
