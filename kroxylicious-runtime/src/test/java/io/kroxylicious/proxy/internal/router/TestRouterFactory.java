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

    @Override
    public Void initialize(RouterFactoryContext context, Void config) {
        return null;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Void initializationData) {
        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(short apiVersion, ApiKeys apiKey,
                                                             RequestHeaderData header, ApiMessage request,
                                                             RouterContext routerContext) {
                return routerContext.respondWithoutReply().withCloseConnection().completed();
            }
        };
    }
}
