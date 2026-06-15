/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.function.IntUnaryOperator;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Timer;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.KafkaProxyExceptionMapper;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.router.CloseOrTerminalStage;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.VirtualNode;
import io.kroxylicious.proxy.service.HostPort;

/**
 * Per-request implementation of {@link RouterContext}. Created by
 * {@link RoutingDecisionHandler} for each incoming client request.
 */
class RouterContextImpl implements RouterContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterContextImpl.class);

    static final int BOOTSTRAP_TARGET_NODE_ID = -1;

    private final int clientCorrelationId;
    private final String sessionId;
    private final Subject subject;
    private final Map<String, RouteDescriptor> routes;
    private final RouteForwarder routeForwarder;
    private final NodeForwarder nodeForwarder;
    private final NodeIdMapping nodeIdMapping;
    private final OptionalInt virtualNodeId;
    private final IntSupplier routingCorrelationIdAllocator;
    private final MeterProvider<Counter> routingRequestsCounter;
    private final MeterProvider<Counter> routingErrorsCounter;
    private final MeterProvider<Timer> routingRequestDurationTimer;
    private final AtomicInteger pendingResponseCount;
    private final PendingResponseRegistry pendingResponseRegistry;
    private final Map<Integer, HostPort> sharedNodeAddresses;
    private final IntUnaryOperator virtualIdTranslator;
    private final Map<Uuid, String> topicIdCache;

    /**
     * Callback interface for forwarding requests to a route's bootstrap server.
     */
    @FunctionalInterface
    interface RouteForwarder {
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

    RouterContextImpl(int clientCorrelationId,
                      String sessionId,
                      Subject subject,
                      Map<String, RouteDescriptor> routes,
                      RouteForwarder routeForwarder,
                      NodeForwarder nodeForwarder,
                      NodeIdMapping nodeIdMapping,
                      OptionalInt virtualNodeId,
                      IntSupplier routingCorrelationIdAllocator,
                      MeterProvider<Counter> routingRequestsCounter,
                      MeterProvider<Counter> routingErrorsCounter,
                      MeterProvider<Timer> routingRequestDurationTimer,
                      AtomicInteger pendingResponseCount,
                      PendingResponseRegistry pendingResponseRegistry,
                      Map<Integer, HostPort> sharedNodeAddresses,
                      IntUnaryOperator virtualIdTranslator,
                      Map<Uuid, String> topicIdCache) {
        this.clientCorrelationId = clientCorrelationId;
        this.sessionId = Objects.requireNonNull(sessionId);
        this.subject = Objects.requireNonNull(subject);
        this.routes = Objects.requireNonNull(routes);
        this.routeForwarder = Objects.requireNonNull(routeForwarder);
        this.nodeForwarder = Objects.requireNonNull(nodeForwarder);
        this.nodeIdMapping = Objects.requireNonNull(nodeIdMapping);
        this.virtualNodeId = Objects.requireNonNull(virtualNodeId);
        this.routingCorrelationIdAllocator = Objects.requireNonNull(routingCorrelationIdAllocator);
        this.routingRequestsCounter = Objects.requireNonNull(routingRequestsCounter);
        this.routingErrorsCounter = Objects.requireNonNull(routingErrorsCounter);
        this.routingRequestDurationTimer = Objects.requireNonNull(routingRequestDurationTimer);
        this.pendingResponseCount = Objects.requireNonNull(pendingResponseCount);
        this.pendingResponseRegistry = Objects.requireNonNull(pendingResponseRegistry);
        this.sharedNodeAddresses = Objects.requireNonNull(sharedNodeAddresses);
        this.virtualIdTranslator = Objects.requireNonNull(virtualIdTranslator);
        this.topicIdCache = Objects.requireNonNull(topicIdCache);
    }

    @Override
    public Optional<VirtualNode> virtualNode() {
        return virtualNodeId.isPresent()
                ? Optional.of(new VirtualNodeImpl(virtualNodeId.getAsInt()))
                : Optional.empty();
    }

    @Override
    public VirtualNode anyNode(String route) {
        if (!routes.containsKey(route)) {
            throw new IllegalArgumentException("Unknown route: " + route);
        }
        return new VirtualNodeImpl(nodeIdMapping.toVirtual(route, BOOTSTRAP_TARGET_NODE_ID));
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
        int virtualNodeId = ((VirtualNodeImpl) node).encodedId();
        NodeIdMapping.RouteAndNode ran = nodeIdMapping.fromVirtual(virtualNodeId);
        String route = ran.route();
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
                    apiKey, requestApiVersion, virtualNodeId, header, request));
        }

        if (!frame.hasResponse()) {
            forwardToNode(virtualNodeId, frame);
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
                    .log("Fire-and-forget request sent to node (no response expected)");
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        Timer.Sample timerSample = Timer.start();
        var pendingResponse = new PendingResponse(
                future, timerSample, route, apiKey,
                nodeIdMapping, createMetadataAddressCacher(route));
        pendingResponseRegistry.register(routingCorrelationId, pendingResponse);
        pendingResponseCount.incrementAndGet();

        try {
            forwardToNode(virtualNodeId, frame);
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
                            null, resp));
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

    private void forwardToNode(int virtualNodeId, DecodedRequestFrame<?> frame) {
        NodeIdMapping.RouteAndNode ran = nodeIdMapping.fromVirtual(virtualNodeId);
        if (ran.targetNodeId() == BOOTSTRAP_TARGET_NODE_ID) {
            routeForwarder.forward(ran.route(), frame);
        }
        else {
            nodeForwarder.forward(virtualNodeId, ran.route(), frame);
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
    public String topicName(Uuid topicId) {
        return topicIdCache.get(topicId);
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

    /**
     * Registers bootstrap addresses for all cluster-targeting routes in the
     * shared node address map so the CCSM can resolve them.
     *
     * @param routes the route descriptors
     * @param nodeIdMapping mapping for this router level
     * @param sharedNodeAddresses shared mutable address map for the CCSM resolver
     * @param virtualIdTranslator translates this level's virtual IDs to outermost virtual IDs
     */
    static void registerBootstrapAddresses(
                                           Map<String, RouteDescriptor> routes,
                                           NodeIdMapping nodeIdMapping,
                                           Map<Integer, HostPort> sharedNodeAddresses,
                                           IntUnaryOperator virtualIdTranslator) {
        for (var entry : routes.entrySet()) {
            RouteDescriptor rd = entry.getValue();
            if (rd.targetsCluster()) {
                int virtualId = nodeIdMapping.toVirtual(entry.getKey(), BOOTSTRAP_TARGET_NODE_ID);
                int outerVirtualId = virtualIdTranslator.applyAsInt(virtualId);
                sharedNodeAddresses.put(outerVirtualId, rd.targetCluster().bootstrapServer());
            }
        }
    }
}
