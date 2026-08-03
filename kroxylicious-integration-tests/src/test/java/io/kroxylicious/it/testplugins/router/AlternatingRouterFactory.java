/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins.router;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.message.ApiVersionsResponseData;
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
import io.kroxylicious.proxy.router.RouterResponse;

/**
 * A test router that alternates PRODUCE requests between two routes in configurable batch sizes.
 * All other API keys are statically routed to {@code routeA}. API_VERSIONS responses are
 * intercepted to cap the PRODUCE version below the threshold where topic IDs replace topic
 * names, since the two backing clusters have independent topic IDs.
 */
@Plugin(configType = AlternatingRouterFactory.Config.class)
public class AlternatingRouterFactory implements RouterFactory<AlternatingRouterFactory.Config, AlternatingRouterFactory.Config> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlternatingRouterFactory.class);

    // PRODUCE v13 replaces topic names with topic IDs (KIP-516).
    // When routing between independent clusters, the IDs differ, so cap at v12.
    private static final short MAX_PRODUCE_VERSION = 12;

    public record Config(String routeA, String routeB, int batchSize) {}

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        String routeA = config.routeA();
        String routeB = config.routeB();
        int batchSize = config.batchSize();
        AtomicInteger counter = new AtomicInteger();

        LOGGER.atInfo()
                .addKeyValue("routeA", routeA)
                .addKeyValue("routeB", routeB)
                .addKeyValue("batchSize", batchSize)
                .log("AlternatingRouter created");

        return new Router() {
            @Override
            public boolean shouldIntercept(ApiKeys apiKey, short apiVersion, RouterContext context) {
                // Intercept all bootstrap traffic.
                // On bound connections, also intercept PRODUCE (for alternating routing) and
                // API_VERSIONS (to cap the PRODUCE version and prevent topic-ID mismatches
                // across independent clusters).
                return context.virtualNode().isEmpty()
                        || apiKey == ApiKeys.PRODUCE
                        || apiKey == ApiKeys.API_VERSIONS;
            }

            @Override
            public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey,
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext ctx) {
                if (apiKey == ApiKeys.API_VERSIONS) {
                    var node = ctx.anyNode(routeA);
                    return ctx.sendRequest(node, header, request)
                            .thenCompose(body -> {
                                capProduceVersion(body);
                                LOGGER.atDebug()
                                        .addKeyValue("sessionId", ctx.sessionId())
                                        .addKeyValue("cappedMaxVersion", MAX_PRODUCE_VERSION)
                                        .log("Capped PRODUCE version in API_VERSIONS response");
                                return ctx.respondWith(body).completed();
                            });
                }

                if (apiKey == ApiKeys.PRODUCE) {
                    int index = counter.getAndIncrement();
                    String route = ((index / batchSize) % 2 == 0) ? routeA : routeB;
                    LOGGER.atDebug()
                            .addKeyValue("sessionId", ctx.sessionId())
                            .addKeyValue("route", route)
                            .addKeyValue("batchIndex", index)
                            .addKeyValue("batchSize", batchSize)
                            .log("Alternating router chose route based on batch index");
                    var node = ctx.anyNode(route);
                    return ctx.sendRequest(node, header, request)
                            .thenCompose(body -> ctx.respondWith(body).completed());
                }

                // All other bootstrap traffic (METADATA, FIND_COORDINATOR, etc.) goes to routeA
                return ctx.sendRequest(ctx.anyNode(routeA), header, request)
                        .thenCompose(body -> ctx.respondWith(body).completed());
            }
        };
    }

    private static void capProduceVersion(ApiMessage responseBody) {
        if (responseBody instanceof ApiVersionsResponseData data) {
            for (var key : data.apiKeys()) {
                if (key.apiKey() == ApiKeys.PRODUCE.id && key.maxVersion() > MAX_PRODUCE_VERSION) {
                    key.setMaxVersion(MAX_PRODUCE_VERSION);
                }
            }
        }
    }
}
