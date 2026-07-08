/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntSupplier;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.router.CloseOrTerminalStage;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.topology.VirtualNode;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Per-request implementation of {@link RouterContext}. Created by
 * {@link RouterDispatchHandler} for each incoming client request.
 */
class RouterContextImpl implements RouterContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterContextImpl.class);
    private static final String LOG_KEY_SESSION_ID = "sessionId";
    private static final String LOG_KEY_ROUTE = "route";
    private static final String LOG_KEY_CLIENT_CORRELATION_ID = "clientCorrelationId";
    private static final String LOG_KEY_ROUTING_CORRELATION_ID = "routingCorrelationId";
    private static final String LOG_KEY_API_VERSION = "apiVersion";
    private static final String LOG_KEY_TARGET_NODE_ID = "targetNodeId";
    private static final String LOG_KEY_ERROR = "error";

    private final int clientCorrelationId;
    private final String sessionId;
    private final Subject subject;
    private final Map<String, RouteDescriptor> routes;
    private final RequestForwarder requestForwarder;
    private final NodeForwarder nodeForwarder;
    private final NodeIdMapping nodeIdMapping;
    private final IntSupplier routingCorrelationIdAllocator;
    private final Channel clientChannel;
    private final long sequenceNumber;
    @Nullable
    private final Integer endpointVirtualNodeId;

    @FunctionalInterface
    interface RequestForwarder {
        void forward(String routeName, Object msg);
    }

    @FunctionalInterface
    interface NodeForwarder {
        void forward(int virtualNodeId, String routeName, Object msg);
    }

    @SuppressWarnings("java:S107")
    RouterContextImpl(DecodedRequestFrame<?> clientFrame,
                      Channel clientChannel,
                      String sessionId,
                      Subject subject,
                      @Nullable Integer endpointVirtualNodeId,
                      Map<String, RouteDescriptor> routes,
                      RequestForwarder requestForwarder,
                      NodeForwarder nodeForwarder,
                      NodeIdMapping nodeIdMapping,
                      IntSupplier routingCorrelationIdAllocator,
                      ResponseSequencer responseSequencer) {
        this.clientCorrelationId = clientFrame.correlationId();
        this.clientChannel = Objects.requireNonNull(clientChannel);
        this.sessionId = Objects.requireNonNull(sessionId);
        this.subject = Objects.requireNonNull(subject);
        this.routes = Objects.requireNonNull(routes);
        this.requestForwarder = Objects.requireNonNull(requestForwarder);
        this.nodeForwarder = Objects.requireNonNull(nodeForwarder);
        this.nodeIdMapping = Objects.requireNonNull(nodeIdMapping);
        this.routingCorrelationIdAllocator = Objects.requireNonNull(routingCorrelationIdAllocator);
        this.sequenceNumber = responseSequencer.allocateSequence();
        this.endpointVirtualNodeId = endpointVirtualNodeId;
    }

    long sequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public Optional<VirtualNode> virtualNode() {
        if (endpointVirtualNodeId == null) {
            return Optional.empty();
        }
        NodeIdMapping.RouteAndNode ran = nodeIdMapping.fromVirtual(endpointVirtualNodeId);
        return Optional.of(new VirtualNodeImpl(ran.route(), ran.targetNodeId()));
    }

    @Override
    public VirtualNode anyNode(String route) {
        if (!routes.containsKey(route)) {
            throw new IllegalArgumentException("Unknown route: " + route);
        }
        return new VirtualNodeImpl(route, null);
    }

    @Override
    public VirtualNode nodeForId(int virtualNodeId) {
        NodeIdMapping.RouteAndNode ran = nodeIdMapping.fromVirtual(virtualNodeId);
        return new VirtualNodeImpl(ran.route(), ran.targetNodeId());
    }

    @Override
    public CompletionStage<ApiMessage> sendRequest(VirtualNode node,
                                                   RequestHeaderData header,
                                                   ApiMessage request) {
        if (!(node instanceof VirtualNodeImpl(String route, Integer nodeId))) {
            throw new IllegalArgumentException("Unrecognised VirtualNode type: " + node.getClass().getName());
        }
        if (nodeId == null) {
            return sendToAnyNode(route, header, request);
        }
        else {
            return sendToSpecificNode(nodeId, route, header, request);
        }
    }

    private CompletionStage<ApiMessage> sendToAnyNode(String route,
                                                      RequestHeaderData header,
                                                      ApiMessage request) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_ROUTE, route)
                    .addKeyValue(LOG_KEY_CLIENT_CORRELATION_ID, clientCorrelationId)
                    .log("Router attempted to send to unknown route");
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown route: " + route));
        }
        if (!rd.targetsCluster()) {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_ROUTE, route)
                    .addKeyValue(LOG_KEY_CLIENT_CORRELATION_ID, clientCorrelationId)
                    .log("Router attempted unsupported nested router route");
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException("Routing to nested routers is not yet supported (route: " + route + ")"));
        }

        ApiKeys apiKey = ApiKeys.forId(header.requestApiKey());
        short requestApiVersion = header.requestApiVersion();
        int routingCorrelationId = routingCorrelationIdAllocator.getAsInt();
        var frame = new DecodedRequestFrame<>(requestApiVersion, routingCorrelationId, true, header, request);

        var listener = RoutingEvent.EVENT_LISTENER.get();
        if (listener != null) {
            listener.accept(new RoutingEvent.Request(sessionId, route, clientCorrelationId, routingCorrelationId, apiKey, requestApiVersion, header, request));
        }

        if (!frame.hasResponse()) {
            requestForwarder.forward(route, frame);
            LOGGER.atTrace()
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_ROUTE, route)
                    .addKeyValue(LOG_KEY_CLIENT_CORRELATION_ID, clientCorrelationId)
                    .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, routingCorrelationId)
                    .log("Fire-and-forget request sent to route (no response expected)");
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        RouterDispatchHandler.registerPendingResponse(clientChannel, routingCorrelationId, new RouterDispatchHandler.PendingResponse(future, route));

        requestForwarder.forward(route, frame);
        LOGGER.atTrace()
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue(LOG_KEY_ROUTE, route)
                .addKeyValue(LOG_KEY_CLIENT_CORRELATION_ID, clientCorrelationId)
                .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, routingCorrelationId)
                .addKeyValue(LOG_KEY_API_VERSION, requestApiVersion)
                .log("Request sent to route");
        attachEventListener(listener, future, route, routingCorrelationId, apiKey);
        return future;
    }

    private CompletionStage<ApiMessage> sendToSpecificNode(int targetNodeId,
                                                           String route,
                                                           RequestHeaderData header,
                                                           ApiMessage request) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null || !rd.targetsCluster()) {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_TARGET_NODE_ID, targetNodeId)
                    .addKeyValue(LOG_KEY_ROUTE, route)
                    .log("Target node resolved to invalid route");
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node " + targetNodeId + " resolved to invalid route: " + route));
        }

        ApiKeys apiKey = ApiKeys.forId(header.requestApiKey());
        short requestApiVersion = header.requestApiVersion();
        int routingCorrelationId = routingCorrelationIdAllocator.getAsInt();
        var frame = new DecodedRequestFrame<>(requestApiVersion, routingCorrelationId, true, header, request);

        var listener = RoutingEvent.EVENT_LISTENER.get();
        if (listener != null) {
            listener.accept(new RoutingEvent.Request(sessionId, route, clientCorrelationId, routingCorrelationId, apiKey, requestApiVersion, header, request));
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        RouterDispatchHandler.registerPendingResponse(clientChannel, routingCorrelationId, new RouterDispatchHandler.PendingResponse(future, route));

        try {
            nodeForwarder.forward(targetNodeId, route, frame);
        }
        catch (Exception e) {
            RouterDispatchHandler.deregisterPendingResponse(clientChannel, routingCorrelationId);
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_TARGET_NODE_ID, targetNodeId)
                    .addKeyValue(LOG_KEY_ROUTE, route)
                    .setCause(LOGGER.isDebugEnabled() ? e : null)
                    .addKeyValue(LOG_KEY_ERROR, e.getMessage())
                    .log(LOGGER.isDebugEnabled()
                            ? "Failed to forward request to node"
                            : "Failed to forward request to node, increase log level to DEBUG for stacktrace");
            return CompletableFuture.failedFuture(e);
        }

        LOGGER.atTrace()
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue(LOG_KEY_ROUTE, route)
                .addKeyValue(LOG_KEY_TARGET_NODE_ID, targetNodeId)
                .addKeyValue(LOG_KEY_CLIENT_CORRELATION_ID, clientCorrelationId)
                .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, routingCorrelationId)
                .log("Request sent to specific node");
        attachEventListener(listener, future, route, routingCorrelationId, apiKey);
        return future;
    }

    private void attachEventListener(java.util.function.Consumer<RoutingEvent> listener,
                                     CompletableFuture<ApiMessage> future,
                                     String route,
                                     int routingCorrelationId,
                                     ApiKeys apiKey) {
        if (listener != null) {
            future.whenComplete((body, error) -> {
                if (body != null) {
                    listener.accept(new RoutingEvent.Response(sessionId, route, routingCorrelationId, apiKey, new ResponseHeaderData(), body));
                }
            });
        }
    }

    @Override
    public String sessionId() {
        return sessionId;
    }

    @Override
    public Subject authenticatedSubject() {
        return subject;
    }

    @Override
    public CloseOrTerminalStage respondWith(ApiMessage body) {
        return RouterResponseImpl.builder(new RouterResponseImpl.RespondWith(null, body, false));
    }

    @Override
    public CloseOrTerminalStage respondWith(ResponseHeaderData header, ApiMessage body) {
        return RouterResponseImpl.builder(new RouterResponseImpl.RespondWith(header, body, false));
    }

    @Override
    public CloseOrTerminalStage respondWithError(RequestHeaderData header,
                                                 ApiMessage request,
                                                 ApiException exception) {
        return RouterResponseImpl.builder(new RouterResponseImpl.RespondWithError(header, request, exception, false));
    }

    @Override
    public CloseOrTerminalStage respondWithoutReply() {
        return RouterResponseImpl.builder(new RouterResponseImpl.RespondWithoutReply(false));
    }
}
