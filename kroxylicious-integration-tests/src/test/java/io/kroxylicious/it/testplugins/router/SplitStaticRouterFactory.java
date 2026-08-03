/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins.router;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletionStage;

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
     * Configuration for the split static router factory.
     *
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
        return new Router() {
            @Override
            public boolean shouldIntercept(ApiKeys apiKey, short apiVersion, RouterContext context) {
                // Intercept on bootstrap to route based on API key
                return context.virtualNode().isEmpty();
            }

            @Override
            public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey,
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext ctx) {
                String route = split.contains(apiKey.name()) ? config.splitRoute() : config.defaultRoute();
                var node = ctx.anyNode(route);
                return ctx.sendRequest(node, header, request)
                        .thenCompose(body -> ctx.respondWith(body).completed());
            }
        };
    }

    @Override
    public void close(Config config) {
    }
}
