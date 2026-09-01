/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.router.CloseOrTerminalStage;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.topology.VirtualNode;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Per-request implementation of {@link RouterContext}. Created by
 * {@link RoutingHandler} for each incoming request at any routing level.
 */
class RouterContextImpl implements RouterContext {

    private final int clientCorrelationId;
    private final String sessionId;
    private final Subject subject;
    private final RouterDispatch dispatch;
    @Nullable
    private final Integer endpointVirtualNodeId;

    RouterContextImpl(DecodedRequestFrame<?> clientFrame,
                      RouterDispatch dispatch,
                      String sessionId,
                      Subject subject,
                      @Nullable Integer endpointVirtualNodeId) {
        this.clientCorrelationId = clientFrame.correlationId();
        this.dispatch = Objects.requireNonNull(dispatch);
        this.sessionId = Objects.requireNonNull(sessionId);
        this.subject = Objects.requireNonNull(subject);
        this.endpointVirtualNodeId = endpointVirtualNodeId;
    }

    @Override
    public Optional<VirtualNode> virtualNode() {
        if (endpointVirtualNodeId == null) {
            return Optional.empty();
        }
        NodeIdMapping.RouteAndNode ran = dispatch.nodeIdMapping().fromVirtual(endpointVirtualNodeId);
        return Optional.of(new VirtualNodeImpl(ran.route(), endpointVirtualNodeId));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the upstream broker already connected to this session for the given route.
     * Multiple calls on the same session always resolve to the same broker — this is not random
     * selection per call.</p>
     */
    @Override
    public VirtualNode anyNode(String route) {
        if (!dispatch.routes().containsKey(route)) {
            throw new IllegalArgumentException("Unknown route: " + route);
        }
        return new VirtualNodeImpl(route, null);
    }

    @Override
    public VirtualNode nodeForId(int virtualNodeId) {
        NodeIdMapping.RouteAndNode ran = dispatch.nodeIdMapping().fromVirtual(virtualNodeId);
        return new VirtualNodeImpl(ran.route(), virtualNodeId);
    }

    @Override
    public CompletionStage<ApiMessage> sendRequest(VirtualNode node,
                                                   RequestHeaderData header,
                                                   ApiMessage request) {
        if (!(node instanceof VirtualNodeImpl(String route, Integer virtualNodeId))) {
            throw new IllegalArgumentException("Unrecognised VirtualNode type: " + node.getClass().getName());
        }
        if (virtualNodeId == null) {
            return dispatch.sendToAnyNode(route, header, request, sessionId, clientCorrelationId);
        }
        else {
            return dispatch.sendToSpecificNode(virtualNodeId, route, header, request, sessionId, clientCorrelationId);
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
                                                 Errors error) {
        return respondWithError(header, request, error, null);
    }

    @Override
    public CloseOrTerminalStage respondWithError(RequestHeaderData header,
                                                 ApiMessage request,
                                                 Errors error,
                                                 @Nullable String message) {
        Objects.requireNonNull(error, "error must not be null");
        if (error == Errors.NONE) {
            throw new IllegalArgumentException("error must denote an actual error, but was Errors.NONE");
        }
        return RouterResponseImpl.builder(new RouterResponseImpl.RespondWithError(header, request, error, message, false));
    }

    @Override
    public CloseOrTerminalStage respondWithoutReply() {
        return RouterResponseImpl.builder(new RouterResponseImpl.RespondWithoutReply(false));
    }
}
