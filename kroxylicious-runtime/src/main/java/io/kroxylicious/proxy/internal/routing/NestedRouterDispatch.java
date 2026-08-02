/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.CorrelationIdAllocator;

/**
 * {@link RouterDispatch} implementation for nested routing levels.
 * Translates local route names to qualified pipeline names and fires
 * frames downstream through the pipeline for processing by inner
 * route filters and {@link RoutingTerminalHandler}.
 */
class NestedRouterDispatch implements RouterDispatch {

    private static final Logger LOGGER = LoggerFactory.getLogger(NestedRouterDispatch.class);
    private static final String LOG_KEY_SESSION_ID = "sessionId";
    private static final String LOG_KEY_ROUTE = "route";
    private static final String LOG_KEY_ROUTING_CORRELATION_ID = "routingCorrelationId";
    private static final String LOG_KEY_ROUTER = "router";
    private static final String LOG_KEY_TARGET_NODE_ID = "targetNodeId";

    private final Map<String, RouteDescriptor> nestedRoutes;
    private final NodeIdMapping nestedNodeIdMapping;
    private final String routerName;
    private final CorrelationIdAllocator correlationIdAllocator;
    private final Map<Integer, RouterDispatchHandler.PendingResponse> pendingResponses;
    private final ChannelHandlerContext ctx;

    NestedRouterDispatch(Map<String, RouteDescriptor> nestedRoutes,
                         NodeIdMapping nestedNodeIdMapping,
                         String routerName,
                         CorrelationIdAllocator correlationIdAllocator,
                         Map<Integer, RouterDispatchHandler.PendingResponse> pendingResponses,
                         ChannelHandlerContext ctx) {
        this.nestedRoutes = nestedRoutes;
        this.nestedNodeIdMapping = nestedNodeIdMapping;
        this.routerName = routerName;
        this.correlationIdAllocator = correlationIdAllocator;
        this.pendingResponses = pendingResponses;
        this.ctx = ctx;
    }

    @Override
    public Map<String, RouteDescriptor> routes() {
        return nestedRoutes;
    }

    @Override
    public NodeIdMapping nodeIdMapping() {
        return nestedNodeIdMapping;
    }

    @Override
    public CompletionStage<ApiMessage> sendToAnyNode(String route,
                                                     RequestHeaderData header,
                                                     ApiMessage request,
                                                     String sessionId,
                                                     int clientCorrelationId) {
        return executeOnEventLoop(() -> doSendToAnyNode(route, header, request, sessionId, clientCorrelationId));
    }

    private CompletableFuture<ApiMessage> doSendToAnyNode(String route,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          String sessionId,
                                                          int clientCorrelationId) {
        RouteDescriptor rd = nestedRoutes.get(route);
        if (rd == null) {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_ROUTE, route)
                    .addKeyValue(LOG_KEY_ROUTER, routerName)
                    .log("Nested router attempted to send to unknown route");
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown route: " + route));
        }
        String qualifiedRoute = routerName + "/" + route;
        short requestApiVersion = header.requestApiVersion();
        int routingCorrelationId = correlationIdAllocator.allocateId();
        var frame = new DecodedRequestFrame<>(requestApiVersion, routingCorrelationId, true, header, request);
        frame.setRouteName(qualifiedRoute);

        if (!frame.hasResponse()) {
            ctx.fireChannelRead(frame);
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        pendingResponses.put(routingCorrelationId,
                new RouterDispatchHandler.PendingResponse(future, route, nestedNodeIdMapping));
        ctx.fireChannelRead(frame);

        LOGGER.atTrace()
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue(LOG_KEY_ROUTE, qualifiedRoute)
                .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, routingCorrelationId)
                .log("Nested request sent to route");
        return future;
    }

    @Override
    public CompletionStage<ApiMessage> sendToSpecificNode(int targetNodeId,
                                                          String route,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          String sessionId,
                                                          int clientCorrelationId) {
        return executeOnEventLoop(() -> doSendToSpecificNode(targetNodeId, route, header, request, sessionId, clientCorrelationId));
    }

    private CompletableFuture<ApiMessage> doSendToSpecificNode(int targetNodeId,
                                                               String route,
                                                               RequestHeaderData header,
                                                               ApiMessage request,
                                                               String sessionId,
                                                               int clientCorrelationId) {
        RouteDescriptor rd = nestedRoutes.get(route);
        if (rd == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node " + targetNodeId + " resolved to unknown route: " + route));
        }

        String qualifiedRoute = routerName + "/" + route;
        short requestApiVersion = header.requestApiVersion();
        int routingCorrelationId = correlationIdAllocator.allocateId();
        var frame = new DecodedRequestFrame<>(requestApiVersion, routingCorrelationId, true, header, request);
        frame.setRouteName(qualifiedRoute);
        frame.setTargetVirtualNodeId(targetNodeId);

        if (!frame.hasResponse()) {
            ctx.fireChannelRead(frame);
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        pendingResponses.put(routingCorrelationId,
                new RouterDispatchHandler.PendingResponse(future, route, nestedNodeIdMapping));
        ctx.fireChannelRead(frame);

        LOGGER.atTrace()
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue(LOG_KEY_ROUTE, qualifiedRoute)
                .addKeyValue(LOG_KEY_TARGET_NODE_ID, targetNodeId)
                .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, routingCorrelationId)
                .log("Nested request sent to specific node");
        return future;
    }

    private <T> CompletionStage<T> executeOnEventLoop(Supplier<CompletableFuture<T>> work) {
        var executor = Objects.requireNonNull(ctx, "sendRequest called before handlerAdded").executor();
        if (executor.inEventLoop()) {
            return work.get();
        }
        CompletableFuture<T> bridge = new CompletableFuture<>();
        executor.execute(() -> work.get().whenComplete((r, e) -> {
            if (e != null) {
                bridge.completeExceptionally(e);
            }
            else {
                bridge.complete(r);
            }
        }));
        return bridge;
    }
}
