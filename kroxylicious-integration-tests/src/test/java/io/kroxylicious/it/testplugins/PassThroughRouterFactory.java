/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

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
 * A router that forwards every request to a single named route, delivering
 * the response back to the client unchanged.
 */
@Plugin(configType = PassThroughRouterFactory.Config.class)
public class PassThroughRouterFactory implements RouterFactory<PassThroughRouterFactory.Config, PassThroughRouterFactory.Config> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PassThroughRouterFactory.class);

    public record Config(String route) {}

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        String route = config.route();
        LOGGER.atInfo()
                .addKeyValue("route", route)
                .log("PassThroughRouter created");
        Map<ApiKeys, String> allStatic = Arrays.stream(ApiKeys.values())
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> route));
        return new Router() {
            @Override
            public CompletionStage<RouterResult> onRequest(
                                                           short apiVersion,
                                                           ApiKeys apiKey,
                                                           RequestHeaderData header,
                                                           ApiMessage request,
                                                           RouterContext routerContext) {
                int nodeId = routerContext.bootstrapNodeId(route);
                return routerContext.sendRequestToNode(route, nodeId, header, request)
                        .thenApply(RouterResult.Completed::new);
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return allStatic;
            }
        };
    }
}
