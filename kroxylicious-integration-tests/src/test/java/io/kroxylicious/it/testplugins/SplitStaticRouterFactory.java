/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
 * A router that statically routes a named subset of API keys to one route and
 * everything else to a default route. Useful for testing multi-upstream static
 * routing without a real Kafka client.
 */
@Plugin(configType = SplitStaticRouterFactory.Config.class)
public class SplitStaticRouterFactory implements RouterFactory<SplitStaticRouterFactory.Config, SplitStaticRouterFactory.Config> {

    /**
     * @param defaultRoute route name for all API keys not listed in {@code splitApiKeys}
     * @param splitRoute   route name for API keys listed in {@code splitApiKeys}
     * @param splitApiKeys names of {@link ApiKeys} constants to send to {@code splitRoute}
     */
    public record Config(String defaultRoute, String splitRoute, List<String> splitApiKeys) {}

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        var split = new HashSet<>(config.splitApiKeys());
        Map<ApiKeys, String> routes = Arrays.stream(ApiKeys.values())
                .collect(Collectors.toUnmodifiableMap(k -> k,
                        k -> split.contains(k.name()) ? config.splitRoute() : config.defaultRoute()));
        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey,
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext routerContext) {
                throw new IllegalStateException("Dynamic routing is not supported");
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return routes;
            }
        };
    }

    @Override
    public void close(Config config) {
    }
}
