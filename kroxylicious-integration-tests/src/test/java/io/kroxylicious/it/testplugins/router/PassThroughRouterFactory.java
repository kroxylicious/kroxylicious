/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins.router;

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
import io.kroxylicious.proxy.topology.VirtualNode;

/**
 * A test router that forwards every bootstrap request to a single named route,
 * delivering the response back to the client. On bound connections the default
 * {@code shouldIntercept} behaviour returns {@code false}, so frames pass through
 * directly to the assigned broker without calling {@code onRequest}.
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
        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(
                                                             ApiKeys apiKey,
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext routerContext) {
                // Use virtualNode() if bound, anyNode(route) if bootstrap.
                VirtualNode target = routerContext.virtualNode()
                        .orElseGet(() -> routerContext.anyNode(config.route()));
                return routerContext.sendRequest(target, header, request)
                        .thenCompose(response -> response == null
                                ? routerContext.respondWithoutReply().completed()
                                : routerContext.respondWith(response).completed());
            }
        };
    }
}
