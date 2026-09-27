/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.topology.VirtualNode;

/**
 * Routes each request on a connection to the route selected for the connection's authenticated
 * subject. Per-connection state; not shared across connections.
 */
class SubjectRoutingHandler implements Router {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubjectRoutingHandler.class);

    private final RouteSelector<Object> selector;
    private final RouteSelectorContext selectorContext;
    private final String virtualClusterName;
    private final String routerName;

    SubjectRoutingHandler(RouteSelector<Object> selector, RouteSelectorContext selectorContext,
                          String virtualClusterName, String routerName) {
        this.selector = selector;
        this.selectorContext = selectorContext;
        this.virtualClusterName = virtualClusterName;
        this.routerName = routerName;
    }

    @Override
    public Map<ApiKeys, String> staticRoutes() {
        return Map.of(); // all dynamic
    }

    @Override
    @SuppressWarnings({ "java:S5738", "removal" })
    public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey, short apiVersion,
                                                     RequestHeaderData header, ApiMessage request,
                                                     RouterContext ctx) {
        Subject subject = ctx.authenticatedSubject();
        if (subject.isAnonymous()) {
            return reject(ctx, header, request, "anonymous connection");
        }
        return selector.selectRoute(subject, selectorContext).thenCompose(routeOpt -> {
            if (routeOpt.isEmpty()) {
                return reject(ctx, header, request, "no route for subject");
            }
            String route = routeOpt.get();
            VirtualNode node = ctx.anyNode(route);
            return ctx.sendRequest(node, header, request)
                    .thenCompose(response -> ctx.respondWith(response).completed());
        }).exceptionallyCompose(err -> reject(ctx, header, request, "selector error"));
    }

    private CompletionStage<RouterResponse> reject(RouterContext ctx, RequestHeaderData header,
                                                   ApiMessage request, String reason) {
        LOGGER.atDebug()
                .addKeyValue("sessionId", ctx.sessionId())
                .addKeyValue("virtualCluster", virtualClusterName)
                .addKeyValue("router", routerName)
                .addKeyValue("reason", reason)
                .log("rejecting request");
        return ctx.respondWithError(header, request, Errors.SASL_AUTHENTICATION_FAILED)
                .withCloseConnection().completed();
    }
}
