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

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.routing.Router;
import io.kroxylicious.proxy.routing.RouterFactory;
import io.kroxylicious.proxy.routing.RouterFactoryContext;
import io.kroxylicious.proxy.routing.RoutingContext;
import io.kroxylicious.proxy.routing.RoutingResult;

/**
 * A router that forwards every request to a single named route, delivering
 * the response back to the client unchanged.
 */
@Plugin(configType = PassThroughRouterFactory.Config.class)
public class PassThroughRouterFactory implements RouterFactory<PassThroughRouterFactory.Config, PassThroughRouterFactory.Config> {

    public record Config(String route) {}

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        String route = config.route();
        Map<ApiKeys, String> allStatic = Arrays.stream(ApiKeys.values())
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> route));
        return new Router() {
            @Override
            public CompletionStage<RoutingResult> onClientRequest(
                                                                  short apiVersion,
                                                                  ApiKeys apiKey,
                                                                  RequestHeaderData header,
                                                                  ApiMessage request,
                                                                  RoutingContext routingContext) {
                return routingContext.sendRequest(route, header, request)
                        .thenApply(response -> {
                            routingContext.sendResponse(response);
                            return RoutingResult.completed();
                        });
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return allStatic;
            }
        };
    }
}
