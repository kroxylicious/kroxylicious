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
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.router.Response;
import io.kroxylicious.proxy.router.RouterContext;

/**
 * Per-request implementation of {@link RouterContext}. Created by
 * {@link RoutingDecisionHandler} for each incoming client request.
 */
class RouterContextImpl implements RouterContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterContextImpl.class);

    private final DecodedRequestFrame<?> clientFrame;
    private final int clientCorrelationId;
    private final short apiVersion;
    private final String sessionId;
    private final Subject subject;
    private final Map<String, RouteDescriptor> routes;
    private final RequestForwarder requestForwarder;
    private final NodeForwarder nodeForwarder;
    private final NodeIdMapping nodeIdMapping;
    private final IntSupplier routingCorrelationIdAllocator;
    private final MeterProvider<Counter> routingRequestsCounter;
    private final MeterProvider<Counter> routingErrorsCounter;
    private final MeterProvider<Timer> routingRequestDurationTimer;
    private final AtomicInteger pendingResponseCount;
    private final Channel clientChannel;
    private final PendingResponseRegistry pendingResponseRegistry;
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

    /**
     * Callback interface for forwarding requests to a specific broker
     * identified by virtual node ID.
     */
    @FunctionalInterface
    interface NodeForwarder {
        void forward(int virtualNodeId, String routeName, Object msg);
    }

    RouterContextImpl(DecodedRequestFrame<?> clientFrame,
                      Channel clientChannel,
                      String sessionId,
                      Subject subject,
                      Map<String, RouteDescriptor> routes,
                      RequestForwarder requestForwarder,
                      NodeForwarder nodeForwarder,
                      NodeIdMapping nodeIdMapping,
                      IntSupplier routingCorrelationIdAllocator,
                      MeterProvider<Counter> routingRequestsCounter,
                      MeterProvider<Counter> routingErrorsCounter,
                      MeterProvider<Timer> routingRequestDurationTimer,
                      AtomicInteger pendingResponseCount,
                      PendingResponseRegistry pendingResponseRegistry,
                      ResponseSequencer responseSequencer,
                      Map<Integer, HostPort> sharedNodeAddresses,
                      IntUnaryOperator virtualIdTranslator,
                      @Nullable NestedRouterProvider nestedRouterProvider) {
        this.clientFrame = Objects.requireNonNull(clientFrame);
        this.clientCorrelationId = clientFrame.correlationId();
        this.apiVersion = clientFrame.apiVersion();
        this.clientChannel = Objects.requireNonNull(clientChannel);
        this.sessionId = Objects.requireNonNull(sessionId);
        this.subject = Objects.requireNonNull(subject);
        this.routes = Objects.requireNonNull(routes);
        this.requestForwarder = Objects.requireNonNull(requestForwarder);
        this.nodeForwarder = Objects.requireNonNull(nodeForwarder);
        this.nodeIdMapping = Objects.requireNonNull(nodeIdMapping);
        this.routingCorrelationIdAllocator = Objects.requireNonNull(routingCorrelationIdAllocator);
        this.routingRequestsCounter = Objects.requireNonNull(routingRequestsCounter);
        this.routingErrorsCounter = Objects.requireNonNull(routingErrorsCounter);
        this.routingRequestDurationTimer = Objects.requireNonNull(routingRequestDurationTimer);
        this.pendingResponseCount = Objects.requireNonNull(pendingResponseCount);
        this.pendingResponseRegistry = Objects.requireNonNull(pendingResponseRegistry);
        this.responseSequencer = Objects.requireNonNull(responseSequencer);
        this.sequenceNumber = responseSequencer.allocateSequence();
    }

    @Override
    public int bootstrapNodeId(String route) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            throw new IllegalArgumentException("Unknown route: " + route);
        }
        return 0;
    }

    @Override
    public CompletionStage<Response> sendRequestToNode(
                                                       String route,
                                                       int virtualNodeId,
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
        short requestApiVersion = header.requestApiVersion();
        int routingCorrelationId = routingCorrelationIdAllocator.getAsInt();
        var frame = new DecodedRequestFrame<>(
                requestApiVersion,
                routingCorrelationId,
                true,
                header,
                request);

        var listener = RoutingEvent.EVENT_LISTENER.get();
        if (listener != null) {
            listener.accept(new RoutingEvent.Request(
                    sessionId, route, clientCorrelationId, routingCorrelationId,
                    apiKey, requestApiVersion, header, request));
        }

        if (!frame.hasResponse()) {
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
                    .log("Fire-and-forget request sent to route (no response expected)");
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Response> future = new CompletableFuture<>();
        Timer.Sample timerSample = Timer.start();
        var pendingResponse = new PendingResponse(
                future, timerSample, route, apiKey,
                nodeIdMapping, createMetadataAddressCacher(route));
        pendingResponseRegistry.register(routingCorrelationId, pendingResponse);
        pendingResponseCount.incrementAndGet();

        try {
            nodeForwarder.forward(virtualNodeId, route, frame);
        }
        catch (Exception e) {
            pendingResponseRegistry.deregister(routingCorrelationId);
            pendingResponseCount.decrementAndGet();
            routingErrorsCounter.withTags(
                    Metrics.ERROR_TYPE_LABEL, "node_forward_failed").increment();
            LOGGER.atWarn()
                    .addKeyValue("sessionId", sessionId)
                    .addKeyValue("virtualNodeId", virtualNodeId)
                    .addKeyValue("route", route)
                    .setCause(LOGGER.isDebugEnabled() ? e : null)
                    .addKeyValue("error", e.getMessage())
                    .log(LOGGER.isDebugEnabled()
                            ? "Failed to forward request to node"
                            : "Failed to forward request to node, increase log level to DEBUG for stacktrace");
            return CompletableFuture.failedFuture(e);
        }

        routingRequestsCounter.withTags(
                Metrics.ROUTE_LABEL, route,
                Metrics.ROUTING_MODE_LABEL, "dynamic",
                Metrics.API_KEY_LABEL, apiKey.name()).increment();
        LOGGER.atTrace()
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("route", route)
                .addKeyValue("virtualNodeId", virtualNodeId)
                .addKeyValue("clientCorrelationId", clientCorrelationId)
                .addKeyValue("routingCorrelationId", routingCorrelationId)
                .log("Request sent to specific node");
        if (listener != null) {
            future.whenComplete((resp, error) -> {
                if (resp != null) {
                    listener.accept(new RoutingEvent.Response(
                            sessionId, route, routingCorrelationId, apiKey,
                            resp.header(), resp.body()));
                }
            });
        }
        return future;
    }

    private MetadataAddressCacher createMetadataAddressCacher(String route) {
        return body -> {
            if (body instanceof MetadataResponseData md) {
                for (var broker : md.brokers()) {
                    int virtualId = nodeIdMapping.toVirtual(route, broker.nodeId());
                    int outerVirtualId = virtualIdTranslator.applyAsInt(virtualId);
                    sharedNodeAddresses.put(outerVirtualId, new HostPort(broker.host(), broker.port()));
                }
            }
        };
    }

    private CompletionStage<Response> dispatchToNestedRouter(
                                                             RouteDescriptor rd,
                                                             String outerRoute,
                                                             RequestHeaderData header,
                                                             ApiMessage request) {
        if (nestedRouterProvider == null) {
            routingErrorsCounter.withTags(
                    Metrics.ERROR_TYPE_LABEL, "nested_routing_unavailable").increment();
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException(
                            "Nested routing is not configured (route: " + outerRoute + ")"));
        }
        NestedRouterState nested = nestedRouterProvider.get(rd.routerName(), outerRoute);
        IntUnaryOperator nestedTranslator = nested.virtualIdTranslator();

        NodeForwarder nestedNodeForwarder = (nestedVirtualNodeId, routeName, msg) -> {
            int outerVirtualId = nestedTranslator.applyAsInt(nestedVirtualNodeId);
            nodeForwarder.forward(outerVirtualId, outerRoute, msg);
        };
        RouteForwarder nestedRouteForwarder = (routeName, msg) -> {
            Integer bootstrapVirtual = nested.bootstrapVirtualNodeIds().get(routeName);
            if (bootstrapVirtual != null) {
                int outerVirtualId = nestedTranslator.applyAsInt(bootstrapVirtual);
                nodeForwarder.forward(outerVirtualId, outerRoute, msg);
            }
        };

        var nestedCtx = new RouterContextImpl(
                clientFrame,
                clientChannel,
                sessionId,
                subject,
                nested.routes(),
                nestedRouteForwarder,
                nestedNodeForwarder,
                nested.nodeIdMapping(),
                nested.bootstrapVirtualNodeIds(),
                routingCorrelationIdAllocator,
                routingRequestsCounter,
                routingErrorsCounter,
                routingRequestDurationTimer,
                pendingResponseCount,
                pendingResponseRegistry,
                responseSequencer,
                sharedNodeAddresses,
                nestedTranslator,
                nested.childProvider());

        ApiKeys apiKey = ApiKeys.forId(header.requestApiKey());
        LOGGER.atTrace()
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("outerRoute", outerRoute)
                .addKeyValue("nestedRouter", rd.routerName())
                .addKeyValue("apiKey", apiKey)
                .log("Dispatching to nested router");

        return nested.router().onRequest(
                header.requestApiVersion(), apiKey, header, request, nestedCtx)
                .thenCompose(result -> {
                    if (result instanceof RouterResult.Completed completed) {
                        return CompletableFuture.completedFuture(completed.response());
                    }
                    else if (result instanceof RouterResult.CompletedNoResponse) {
                        return CompletableFuture.completedFuture(null);
                    }
                    else if (result instanceof RouterResult.Disconnect) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException(
                                        "Nested router '" + rd.routerName()
                                                + "' attempted to disconnect client"));
                    }
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Unknown router result type: " + result));
                });
    }

    private void forwardToNode(int virtualNodeId, String route, DecodedRequestFrame<?> frame) {
        Integer bootstrapId = bootstrapVirtualNodeIds.get(route);
        if (bootstrapId != null && bootstrapId == virtualNodeId) {
            routeForwarder.forward(route, frame);
        }
        else {
            nodeForwarder.forward(virtualNodeId, route, frame);
        }
    }

    /**
     * Submits a response to the client via the response sequencer.
     * Called by {@link RoutingDecisionHandler} when the router returns
     * {@link io.kroxylicious.proxy.router.RouterResult.Completed}.
     */
    void submitResponse(Response response) {
        response.header().setCorrelationId(clientCorrelationId);
        var responseFrame = clientFrame.responseFrame(response.header(), response.body());
        responseSequencer.submit(sequenceNumber, responseFrame);
        LOGGER.atTrace()
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("clientCorrelationId", clientCorrelationId)
                .addKeyValue("sequenceNumber", sequenceNumber)
                .log("Response submitted to sequencer");
    }

    /**
     * Closes the client channel. Called by {@link RoutingDecisionHandler}
     * when the router returns {@link io.kroxylicious.proxy.router.RouterResult.Disconnect}.
     */
    void disconnectClient() {
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
