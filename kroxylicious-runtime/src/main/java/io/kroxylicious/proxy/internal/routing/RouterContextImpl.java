/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.router.CloseOrTerminalStage;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.topology.Bootstrap;
import io.kroxylicious.proxy.topology.EndpointType;
import io.kroxylicious.proxy.topology.VirtualNode;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Per-request implementation of {@link RouterContext}. Created by
 * {@link RouterDispatchHandler} for each incoming client request.
 */
class RouterContextImpl implements RouterContext {

    private final int clientCorrelationId;
    private final String sessionId;
    private final Subject subject;
    private final RouterDispatchHandler handler;
    @Nullable
    private final Integer endpointVirtualNodeId;

    RouterContextImpl(DecodedRequestFrame<?> clientFrame,
                      RouterDispatchHandler handler,
                      String sessionId,
                      Subject subject,
                      @Nullable Integer endpointVirtualNodeId) {
        this.clientCorrelationId = clientFrame.correlationId();
        this.handler = Objects.requireNonNull(handler);
        this.sessionId = Objects.requireNonNull(sessionId);
        this.subject = Objects.requireNonNull(subject);
        this.endpointVirtualNodeId = endpointVirtualNodeId;
    }

    @Override
    public EndpointType endpoint() {
        if (endpointVirtualNodeId == null) {
            return new EndpointType.Bootstrap();
        }
        return new EndpointType.VirtualNode(endpointVirtualNodeId);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the upstream broker already connected to this session for the given route.
     * Multiple calls on the same session always resolve to the same broker — this is not random
     * selection per call.</p>
     */
    @Override
    public EndpointType.VirtualNode nodeForId(int virtualNodeId) {
        return new EndpointType.VirtualNode(virtualNodeId);
    }

    @Override
    public CompletionStage<ApiMessage> sendRequest(EndpointType.VirtualNode node,
                                                   RequestHeaderData header,
                                                   ApiMessage request) {
        NodeIdMapping.RouteAndNode ran = handler.nodeIdMapping.fromVirtual(node.downstreamNodeId());
        return handler.sendToSpecificNode(node.downstreamNodeId(), ran.route(), header, request, sessionId, clientCorrelationId);
    }

    @Override
    public CompletionStage<ApiMessage> sendToRoute(String route,
                                                   RequestHeaderData header,
                                                   ApiMessage request) {
        if (!handler.routes.containsKey(route)) {
            throw new IllegalArgumentException("Unknown route: " + route);
        }
        return handler.sendToAnyNode(route, header, request, sessionId, clientCorrelationId);
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
