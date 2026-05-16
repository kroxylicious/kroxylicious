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

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;

import io.netty.channel.Channel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.KafkaProxyExceptionMapper;
import io.kroxylicious.proxy.router.CloseOrTerminalStage;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.topology.VirtualNode;

/**
 * Per-request implementation of {@link RouterContext}. Created by
 * {@link RouterDispatchHandler} for each incoming client request.
 */
class RoutingContextImpl implements RouterContext {

    private final int clientCorrelationId;
    private final short apiVersion;
    private final Channel clientChannel;
    private final String sessionId;
    private final Subject subject;
    private final Map<String, RouteDescriptor> routes;
    private final RequestForwarder requestForwarder;

    @FunctionalInterface
    interface RequestForwarder {
        void forward(String routeName, Object msg);
    }

    RoutingContextImpl(int clientCorrelationId,
                       short apiVersion,
                       Channel clientChannel,
                       String sessionId,
                       Subject subject,
                       Map<String, RouteDescriptor> routes,
                       RequestForwarder requestForwarder) {
        this.clientCorrelationId = clientCorrelationId;
        this.apiVersion = apiVersion;
        this.clientChannel = Objects.requireNonNull(clientChannel);
        this.sessionId = Objects.requireNonNull(sessionId);
        this.subject = Objects.requireNonNull(subject);
        this.routes = Objects.requireNonNull(routes);
        this.requestForwarder = Objects.requireNonNull(requestForwarder);
    }

    @Override
    public Optional<VirtualNode> virtualNode() {
        return Optional.empty();
    }

    @Override
    public VirtualNode anyNode(String route) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            throw new IllegalArgumentException("Unknown route: " + route);
        }
        return new VirtualNodeImpl(rd.id());
    }

    @Override
    public VirtualNode nodeForId(int virtualNodeId) {
        return new VirtualNodeImpl(virtualNodeId);
    }

    @Override
    public CompletionStage<ApiMessage> sendRequest(
                                                   VirtualNode node,
                                                   RequestHeaderData header,
                                                   ApiMessage request) {
        VirtualNodeImpl vni = (VirtualNodeImpl) node;
        RouteDescriptor rd = findRouteForNode(vni);
        if (!rd.targetsCluster()) {
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException(
                            "Routing to nested routers is not yet supported (route: " + rd.name() + ")"));
        }

        var frame = new DecodedRequestFrame<>(
                apiVersion,
                clientCorrelationId,
                true,
                header,
                request);

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        RouterDispatchHandler.registerPendingResponse(clientChannel, clientCorrelationId, future);

        requestForwarder.forward(rd.name(), frame);
        return future;
    }

    private RouteDescriptor findRouteForNode(VirtualNodeImpl node) {
        return routes.values().stream()
                .filter(rd -> rd.id() == node.encodedId())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No route found for virtual node ID: " + node.encodedId()));
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
        Objects.requireNonNull(body);
        return new RouterResultBuilderImpl(null, body);
    }

    @Override
    public CloseOrTerminalStage respondWith(
                                            ResponseHeaderData header,
                                            ApiMessage body) {
        return new RouterResultBuilderImpl(header, body);
    }

    @Override
    public CloseOrTerminalStage respondWithError(
                                                 RequestHeaderData header,
                                                 ApiMessage request,
                                                 ApiException exception) {
        AbstractResponse errorResponse = KafkaProxyExceptionMapper
                .errorResponseForMessage(header, request, exception);
        ResponseHeaderData responseHeader = new ResponseHeaderData();
        responseHeader.setCorrelationId(header.correlationId());
        return new RouterResultBuilderImpl(responseHeader, errorResponse.data());
    }

    @Override
    public CloseOrTerminalStage respondWithoutReply() {
        return new RouterResultBuilderImpl(null, null);
    }
}
