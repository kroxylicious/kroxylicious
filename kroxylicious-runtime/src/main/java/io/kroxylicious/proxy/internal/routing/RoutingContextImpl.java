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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.Channel;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.routing.Response;
import io.kroxylicious.proxy.routing.RoutingContext;

/**
 * Per-request implementation of {@link RoutingContext}. Created by
 * {@link RouterDispatchHandler} for each incoming client request.
 */
class RoutingContextImpl implements RoutingContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingContextImpl.class);

    private final int clientCorrelationId;
    private final short apiVersion;
    private final String sessionId;
    private final Subject subject;
    private final Map<String, RouteDescriptor> routes;
    private final RequestForwarder requestForwarder;
    private final IntSupplier routingCorrelationIdAllocator;
    private final MeterProvider<Counter> routingRequestsCounter;
    private final MeterProvider<Counter> routingErrorsCounter;
    private final MeterProvider<Timer> routingRequestDurationTimer;
    private final AtomicInteger pendingResponseCount;
    private final Channel clientChannel;
    private final ResponseSequencer responseSequencer;
    private final long sequenceNumber;

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
                       RequestForwarder requestForwarder,
                       IntSupplier routingCorrelationIdAllocator,
                       MeterProvider<Counter> routingRequestsCounter,
                       MeterProvider<Counter> routingErrorsCounter,
                       MeterProvider<Timer> routingRequestDurationTimer,
                       AtomicInteger pendingResponseCount,
                       ResponseSequencer responseSequencer) {
        this.clientCorrelationId = clientCorrelationId;
        this.apiVersion = apiVersion;
        this.clientChannel = Objects.requireNonNull(clientChannel);
        this.sessionId = Objects.requireNonNull(sessionId);
        this.subject = Objects.requireNonNull(subject);
        this.routes = Objects.requireNonNull(routes);
        this.requestForwarder = Objects.requireNonNull(requestForwarder);
        this.routingCorrelationIdAllocator = Objects.requireNonNull(routingCorrelationIdAllocator);
        this.routingRequestsCounter = Objects.requireNonNull(routingRequestsCounter);
        this.routingErrorsCounter = Objects.requireNonNull(routingErrorsCounter);
        this.routingRequestDurationTimer = Objects.requireNonNull(routingRequestDurationTimer);
        this.pendingResponseCount = Objects.requireNonNull(pendingResponseCount);
        this.responseSequencer = Objects.requireNonNull(responseSequencer);
        this.sequenceNumber = responseSequencer.allocateSequence();
    }

    @Override
    public CompletionStage<Response> sendRequest(
                                                 String route,
                                                 RequestHeaderData header,
                                                 ApiMessage request) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            routingErrorsCounter.withTags(
                    Metrics.ERROR_TYPE_LABEL, "unknown_route").increment();
            LOGGER.atWarn()
                    .addKeyValue("sessionId", sessionId)
                    .addKeyValue("route", route)
                    .addKeyValue("clientCorrelationId", clientCorrelationId)
                    .log("Router attempted to send to unknown route");
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Unknown route: " + route));
        }
        if (!rd.targetsCluster()) {
            routingErrorsCounter.withTags(
                    Metrics.ERROR_TYPE_LABEL, "unsupported_nested_router").increment();
            LOGGER.atWarn()
                    .addKeyValue("sessionId", sessionId)
                    .addKeyValue("route", route)
                    .addKeyValue("clientCorrelationId", clientCorrelationId)
                    .log("Router attempted unsupported nested router route");
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException(
                            "Routing to nested routers is not yet supported (route: " + route + ")"));
        }

        ApiKeys apiKey = ApiKeys.forId(header.requestApiKey());
        int routingCorrelationId = routingCorrelationIdAllocator.getAsInt();
        var frame = new DecodedRequestFrame<>(
                apiVersion,
                routingCorrelationId,
                true,
                header,
                request);

        CompletableFuture<Response> future = new CompletableFuture<>();
        Timer.Sample timerSample = Timer.start();
        var pendingResponse = new RouterDispatchHandler.PendingResponse(
                future, timerSample, route, apiKey);
        RouterDispatchHandler.registerPendingResponse(
                clientChannel, routingCorrelationId, pendingResponse);
        pendingResponseCount.incrementAndGet();

        requestForwarder.forward(route, frame);
        routingRequestsCounter.withTags(
                Metrics.ROUTE_LABEL, route,
                Metrics.ROUTING_MODE_LABEL, "dynamic",
                Metrics.API_KEY_LABEL, apiKey.name()).increment();
        LOGGER.atTrace()
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("route", route)
                .addKeyValue("clientCorrelationId", clientCorrelationId)
                .addKeyValue("routingCorrelationId", routingCorrelationId)
                .addKeyValue("apiVersion", apiVersion)
                .log("Request sent to route");
        return future;
    }

    @Override
    public void sendResponse(Response response) {
        var responseFrame = new DecodedResponseFrame<>(
                apiVersion,
                clientCorrelationId,
                response.header(),
                response.body());
        responseSequencer.submit(sequenceNumber, responseFrame);
        LOGGER.atTrace()
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("clientCorrelationId", clientCorrelationId)
                .addKeyValue("sequenceNumber", sequenceNumber)
                .log("Response submitted to sequencer");
    }

    @Override
    public void disconnect() {
        LOGGER.atDebug()
                .addKeyValue("sessionId", sessionId)
                .log("Router requested client disconnect");
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
