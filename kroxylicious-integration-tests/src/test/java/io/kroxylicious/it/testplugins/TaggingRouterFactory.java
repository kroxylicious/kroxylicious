/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.RouterResult;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A Router that adds a {@link RawTaggedField} with tag {@value TAG} to the
 * body of every PRODUCE request before forwarding it to a single configured
 * route. Used to prove that downstream route filters observe modifications
 * made by upstream routers.
 */
@Plugin(configType = TaggingRouterFactory.Config.class)
public class TaggingRouterFactory
        implements RouterFactory<TaggingRouterFactory.Config, TaggingRouterFactory.Config> {

    public static final int BASE_TAG = 600;

    public record Config(String routerTag, int tagOffset, String route) {}

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        return new Router() {
            @Override
            public CompletionStage<RouterResult> onRequest(short apiVersion,
                                                           ApiKeys apiKey,
                                                           RequestHeaderData header,
                                                           ApiMessage request,
                                                           RouterContext routerContext) {
                if (apiKey == ApiKeys.PRODUCE) {
                    request.unknownTaggedFields().add(
                            new RawTaggedField(BASE_TAG + config.tagOffset(), config.routerTag().getBytes(UTF_8)));
                }
                int nodeId = routerContext.anyNodeId(config.route());
                return routerContext.sendRequestToNode(nodeId, header, request)
                        .thenApply(RouterResult.Completed::new);
            }
        };
    }
}
