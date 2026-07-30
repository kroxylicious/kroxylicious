/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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
        RouteDescriptor rd = nestedRoutes.get(route);
        if (rd == null) {
            LOGGER.atWarn()
                    .addKeyValue("sessionId", sessionId)
                    .addKeyValue("route", route)
                    .addKeyValue("router", routerName)
                    .log("Nested router attempted to send to unknown route");
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown route: " + route));
        }
        if (!rd.targetsCluster()) {
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException("Deeply nested routers are not yet supported (route: " + route + ")"));
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
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("route", qualifiedRoute)
                .addKeyValue("routingCorrelationId", routingCorrelationId)
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
        RouteDescriptor rd = nestedRoutes.get(route);
        if (rd == null || !rd.targetsCluster()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node " + targetNodeId + " resolved to invalid route: " + route));
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
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("route", qualifiedRoute)
                .addKeyValue("targetNodeId", targetNodeId)
                .addKeyValue("routingCorrelationId", routingCorrelationId)
                .log("Nested request sent to specific node");
        return future;
    }
}
