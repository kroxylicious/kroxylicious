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

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.netty.channel.Channel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.routing.Response;
import io.kroxylicious.proxy.routing.RoutingContext;

/**
 * Per-request implementation of {@link RoutingContext}. Created by
 * {@link RouterDispatchHandler} for each incoming client request.
 */
class RoutingContextImpl implements RoutingContext {

    private final int clientCorrelationId;
    private final short apiVersion;
    private final Channel clientChannel;
    private final String sessionId;
    private final Subject subject;
    private final Map<String, RouteDescriptor> routes;
    private final RequestForwarder requestForwarder;

    /**
     * Callback interface for forwarding requests to the backend. The
     * {@link RouterDispatchHandler} provides an implementation that
     * delegates to the {@link io.kroxylicious.proxy.internal.ClientConnectionStateMachine}.
     */
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
    public CompletionStage<Response> sendRequest(
                                                 String route,
                                                 RequestHeaderData header,
                                                 ApiMessage request) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Unknown route: " + route));
        }
        if (!rd.targetsCluster()) {
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException(
                            "Routing to nested routers is not yet supported (route: " + route + ")"));
        }

        var frame = new DecodedRequestFrame<>(
                apiVersion,
                clientCorrelationId,
                true,
                header,
                request);

        CompletableFuture<Response> future = new CompletableFuture<>();
        RouterDispatchHandler.registerPendingResponse(clientChannel, clientCorrelationId, future);

        requestForwarder.forward(route, frame);
        return future;
    }

    @Override
    public void sendResponse(Response response) {
        var responseFrame = new DecodedResponseFrame<>(
                apiVersion,
                clientCorrelationId,
                response.header(),
                response.body());
        clientChannel.write(responseFrame);
        clientChannel.flush();
    }

    @Override
    public void disconnect() {
        clientChannel.close();
    }

    @Override
    public String sessionId() {
        return sessionId;
    }

    @Override
    public Subject authenticatedSubject() {
        return subject;
    }
}
