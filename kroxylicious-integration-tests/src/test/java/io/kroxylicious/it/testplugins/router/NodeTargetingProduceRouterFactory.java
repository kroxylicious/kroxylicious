/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins.router;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;

/**
 * A test router that targets a specific virtual node ID via
 * {@link RouterContext#nodeForId(int)} for all requests. No static routes —
 * all requests flow through {@code onRequest} to avoid opaque frames reaching
 * nested handlers.
 */
@Plugin(configType = NodeTargetingProduceRouterFactory.Config.class)
public class NodeTargetingProduceRouterFactory
        implements RouterFactory<NodeTargetingProduceRouterFactory.Config, NodeTargetingProduceRouterFactory.Config> {

    public record Config(String route, int targetVirtualNodeId) {}

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        String route = config.route();
        int targetVirtualNodeId = config.targetVirtualNodeId();

        return (apiKey, apiVersion, header, request, ctx) -> {
            var node = ctx.nodeForId(targetVirtualNodeId);
            return ctx.sendRequest(node, header, request)
                    .thenCompose(body -> ctx.respondWith(body).completed());
        };
    }
}
