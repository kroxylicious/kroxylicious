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
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.RouterResponse;

/**
 * A router that forwards every request to a single named route, delivering
 * the response back to the client unchanged.
 */
@Plugin(configType = PassThroughRouterFactory.Config.class)
public class PassThroughRouterFactory implements RouterFactory<PassThroughRouterFactory.Config, PassThroughRouterFactory.Config> {

    public record Config(String route) {}

    private Map<ApiKeys, String> allStatic;

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        allStatic = Arrays.stream(ApiKeys.values())
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> config.route()));
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(
                                                             ApiKeys apiKey,
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext routerContext) {
                throw new IllegalStateException("Dynamic routing is not supported");
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return allStatic;
            }
        };
    }
}
