/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.CorrelationIdAllocator;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Creates request frames and fires them downstream, tracks them by
 * correlation ID, and matches returning response frames back to their
 * pending futures — translating node IDs and caching upstream broker
 * addresses along the way.
 *
 * <p>Parameterised by a route prefix (empty for the top-level router,
 * {@code "routerName/"} for a nested router) so the same logic serves
 * every router in the routing tree.
 *
 * <p>Not thread-safe; all callers must be on the same Netty event loop.
 * Off-loop calls are bridged via {@link #executeOnEventLoop}.
 */
public class RouteDispatcher implements RouterDispatch {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouteDispatcher.class);

    /**
     * API keys whose responses carry node IDs that must be translated to virtual node IDs.
     * These keys are always decoded (even when statically routed) so the response bodies
     * are accessible for translation.
     */
    public static final Set<ApiKeys> NODE_ID_TRANSLATION_APIS = Set.of(
            ApiKeys.METADATA,
            ApiKeys.FIND_COORDINATOR,
            ApiKeys.DESCRIBE_CLUSTER,
            ApiKeys.PRODUCE,
            ApiKeys.FETCH,
            ApiKeys.SHARE_FETCH,
            ApiKeys.SHARE_ACKNOWLEDGE,
            ApiKeys.DESCRIBE_TOPIC_PARTITIONS);

    private static final String LOG_KEY_ROUTING_CORRELATION_ID = "routingCorrelationId";
    private static final String LOG_KEY_TARGET_NODE_ID = "targetNodeId";
    private static final String LOG_KEY_VIRTUAL_CLUSTER = "virtualCluster";
    private static final String LOG_KEY_SESSION_ID = "sessionId";

    private final Map<String, RouteDescriptor> routes;
    private final NodeIdMapping nodeIdMapping;
    private final String routePrefix;
    private final CorrelationIdAllocator correlationIdAllocator;
    private final Map<Integer, HostPort> routerNodeAddresses;
    private final String virtualClusterName;

    private final Map<Integer, PendingResponse> pendingResponses = new HashMap<>();
    private final Map<Integer, String> pendingStaticRoutes = new HashMap<>();

    @Nullable
    private ChannelHandlerContext ctx;

    record PendingResponse(CompletableFuture<ApiMessage> future, String route, NodeIdMapping nodeIdMapping) {}

    enum ResponseOutcome {
        CONSUMED,
        STATIC_TRANSLATED,
        UNHANDLED
    }

    RouteDispatcher(Map<String, RouteDescriptor> routes,
                    NodeIdMapping nodeIdMapping,
                    String routePrefix,
                    CorrelationIdAllocator correlationIdAllocator,
                    Map<Integer, HostPort> routerNodeAddresses,
                    String virtualClusterName) {
        this.routes = routes;
        this.nodeIdMapping = nodeIdMapping;
        this.routePrefix = routePrefix;
        this.correlationIdAllocator = correlationIdAllocator;
        this.routerNodeAddresses = routerNodeAddresses;
        this.virtualClusterName = virtualClusterName;
    }

    void setContext(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public Map<String, RouteDescriptor> routes() {
        return routes;
    }

    @Override
    public NodeIdMapping nodeIdMapping() {
        return nodeIdMapping;
    }

    /**
     * Returns the correlation ID allocator shared by all routing levels on this connection.
     *
     * @return the correlation ID allocator
     */
    public CorrelationIdAllocator correlationIdAllocator() {
        return correlationIdAllocator;
    }

    /**
     * Returns the upstream node addresses known at this routing level, keyed by virtual node ID.
     *
     * @return the known upstream node addresses
     */
    public Map<Integer, HostPort> routerNodeAddresses() {
        return routerNodeAddresses;
    }

    /**
     * Returns the upstream address for the given virtual node ID, as learned from the most
     * recent internal METADATA response. Returns empty if the address has not been cached yet.
     *
     * @param virtualNodeId the virtual node ID to resolve
     * @return the upstream address of the node, or empty if not yet known
     */
    public Optional<HostPort> resolveRouterNodeAddress(int virtualNodeId) {
        return Optional.ofNullable(routerNodeAddresses.get(virtualNodeId));
    }

    String qualifyRoute(String route) {
        return routePrefix + route;
    }

    void trackStaticRoute(int correlationId, String route) {
        pendingStaticRoutes.put(correlationId, route);
    }

    // --- Dispatch ---

    @Override
    public CompletionStage<ApiMessage> sendToAnyNode(String route,
                                                     RequestHeaderData header,
                                                     ApiMessage request,
                                                     String sessionId,
                                                     int clientCorrelationId) {
        return executeOnEventLoop(() -> doSendToAnyNode(route, header, request, sessionId, clientCorrelationId));
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

    private CompletableFuture<ApiMessage> doSendToAnyNode(String route, RequestHeaderData header, ApiMessage request, String sessionId,
                                                          int clientCorrelationId) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            withSendContext(LOGGER.atWarn(), sessionId, route, clientCorrelationId)
                    .log("Router attempted to send to unknown route");
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown route: " + route));
        }
        String qualifiedRoute = qualifyRoute(route);
        short requestApiVersion = header.requestApiVersion();
        int routingCorrelationId = correlationIdAllocator.allocateId();
        var frame = new DecodedRequestFrame<>(requestApiVersion, routingCorrelationId, true, header, request);
        frame.setRouteName(qualifiedRoute);

        if (!frame.hasResponse()) {
            fireChannelRead(frame);
            withSendContext(LOGGER.atTrace(), sessionId, route, clientCorrelationId)
                    .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, routingCorrelationId)
                    .log("Fire-and-forget request sent to route (no response expected)");
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        pendingResponses.put(routingCorrelationId, new PendingResponse(future, route, nodeIdMapping));
        fireChannelRead(frame);

        withSendContext(LOGGER.atTrace(), sessionId, route, clientCorrelationId)
                .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, routingCorrelationId)
                .addKeyValue("apiVersion", requestApiVersion)
                .log("Request sent to route");
        return future;
    }

    private CompletableFuture<ApiMessage> doSendToSpecificNode(int targetNodeId,
                                                               String route,
                                                               RequestHeaderData header,
                                                               ApiMessage request,
                                                               String sessionId,
                                                               int clientCorrelationId) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            withNodeContext(LOGGER.atWarn(), sessionId, route, targetNodeId)
                    .log("Target node resolved to unknown route");
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node " + targetNodeId + " resolved to unknown route: " + route));
        }

        String qualifiedRoute = qualifyRoute(route);
        short requestApiVersion = header.requestApiVersion();
        int routingCorrelationId = correlationIdAllocator.allocateId();
        var frame = new DecodedRequestFrame<>(requestApiVersion, routingCorrelationId, true, header, request);
        frame.setRouteName(qualifiedRoute);
        frame.setTargetVirtualNodeId(targetNodeId);

        if (!frame.hasResponse()) {
            fireChannelRead(frame);
            withSendContext(LOGGER.atTrace(), sessionId, route, clientCorrelationId)
                    .addKeyValue(LOG_KEY_TARGET_NODE_ID, targetNodeId)
                    .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, routingCorrelationId)
                    .log("Fire-and-forget request sent to specific node (no response expected)");
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        pendingResponses.put(routingCorrelationId, new PendingResponse(future, route, nodeIdMapping));
        fireChannelRead(frame);

        withSendContext(LOGGER.atTrace(), sessionId, route, clientCorrelationId)
                .addKeyValue(LOG_KEY_TARGET_NODE_ID, targetNodeId)
                .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, routingCorrelationId)
                .log("Request sent to specific node");
        return future;
    }

    // --- Response correlation ---

    ResponseOutcome handleResponse(DecodedResponseFrame<?> frame, String sessionId) {
        int correlationId = frame.correlationId();
        PendingResponse pending = pendingResponses.remove(correlationId);
        if (pending != null) {
            ApiMessage body = frame.body();
            try {
                NodeIdResponseTranslator.translate(body, frame.apiVersion(),
                        pending.nodeIdMapping(), pending.route());
                cacheNodeAddressesIfMetadata(body, sessionId);
                pending.future().complete(body);
            }
            catch (Exception t) {
                pending.future().completeExceptionally(t);
            }
            finally {
                // Safe: DecodedResponseFrames decoded from the network carry no managed ByteBufs
                // (KafkaResponseDecoder adds none), and RouteFilterHandler only intercepts frames
                // whose routeName matches its route — upstream responses have null routeName and
                // pass through unmodified. So body does not alias any ByteBuf released here.
                frame.release();
            }
            LOGGER.atTrace()
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_ROUTING_CORRELATION_ID, correlationId)
                    .log("Routed response matched to pending request");
            return ResponseOutcome.CONSUMED;
        }

        String staticRoute = pendingStaticRoutes.remove(correlationId);
        if (staticRoute != null) {
            NodeIdResponseTranslator.translate(frame.body(), frame.apiVersion(),
                    nodeIdMapping, staticRoute);
            cacheNodeAddressesIfMetadata(frame.body(), sessionId);
            return ResponseOutcome.STATIC_TRANSLATED;
        }

        return ResponseOutcome.UNHANDLED;
    }

    // --- Lifecycle ---

    @VisibleForTesting
    boolean hasPendingResponses() {
        return !pendingResponses.isEmpty();
    }

    @VisibleForTesting
    void addPendingResponse(int correlationId, PendingResponse pending) {
        pendingResponses.put(correlationId, pending);
    }

    @VisibleForTesting
    PendingResponse getPendingResponse(int correlationId) {
        return pendingResponses.get(correlationId);
    }

    @VisibleForTesting
    int pendingResponseCount() {
        return pendingResponses.size();
    }

    @VisibleForTesting
    void addPendingStaticRoute(int correlationId, String route) {
        pendingStaticRoutes.put(correlationId, route);
    }

    @VisibleForTesting
    boolean hasPendingStaticRoute(int correlationId) {
        return pendingStaticRoutes.containsKey(correlationId);
    }

    void failAllPending(String sessionId) {
        int abandoned = pendingResponses.size();
        if (abandoned > 0) {
            var cause = new IllegalStateException("Connection closed with " + abandoned + " pending router response(s)");
            for (var entry : pendingResponses.values()) {
                entry.future().completeExceptionally(cause);
            }
            pendingResponses.clear();
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue("abandonedResponses", abandoned)
                    .log("Connection closed with pending router responses");
        }
    }

    // --- Internal helpers ---

    // FutureReturnValueIgnored: a synchronous throw from work.get() is caught and
    // propagated to `bridge`, so the submitted task cannot complete exceptionally and the
    // whenComplete callback completes `bridge` in both branches.
    @SuppressWarnings("FutureReturnValueIgnored")
    private <T> CompletionStage<T> executeOnEventLoop(Supplier<CompletableFuture<T>> work) {
        var executor = Objects.requireNonNull(ctx, "sendRequest called before handlerAdded").executor();
        if (executor.inEventLoop()) {
            return work.get();
        }
        CompletableFuture<T> bridge = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                work.get().whenComplete((r, e) -> {
                    if (e != null) {
                        bridge.completeExceptionally(e);
                    }
                    else {
                        bridge.complete(r);
                    }
                });
            }
            catch (Exception t) {
                bridge.completeExceptionally(t);
            }
        });
        return bridge;
    }

    private void fireChannelRead(Object msg) {
        Objects.requireNonNull(ctx, "fireChannelRead called before handlerAdded").fireChannelRead(msg);
    }

    private void cacheNodeAddressesIfMetadata(Object body, String sessionId) {
        if (body instanceof MetadataResponseData md) {
            for (var broker : md.brokers()) {
                routerNodeAddresses.put(broker.nodeId(), new HostPort(broker.host(), broker.port()));
            }
            if (!md.brokers().isEmpty()) {
                LOGGER.atDebug()
                        .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                        .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                        .addKeyValue("brokerCount", md.brokers().size())
                        .log("Cached upstream node addresses from internal METADATA response");
            }
        }
    }

    private LoggingEventBuilder withSendContext(LoggingEventBuilder event, String sessionId, String route, int clientCorrelationId) {
        return event.addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue("route", route)
                .addKeyValue("clientCorrelationId", clientCorrelationId);
    }

    private LoggingEventBuilder withNodeContext(LoggingEventBuilder event, String sessionId, String route, int targetNodeId) {
        return event.addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue(LOG_KEY_TARGET_NODE_ID, targetNodeId)
                .addKeyValue("route", route);
    }
}
