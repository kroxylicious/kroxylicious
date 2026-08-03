/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router;

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

@Plugin(configType = Void.class)
public class TestRouterFactory implements RouterFactory<Void, Void> {

    public static final String DEFAULT_ROUTE = "default";

    @Override
    public Void initialize(RouterFactoryContext context, Void config) {
        return null;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Void initializationData) {
        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey, short apiVersion,
                                                             RequestHeaderData header, ApiMessage request,
                                                             RouterContext routerContext) {
                throw new IllegalStateException("Dynamic routing is not supported");
            }

            @Override
            public boolean shouldIntercept(ApiKeys apiKey, short apiVersion, RouterContext context) {
                return false;
            }
        };
    }
}
