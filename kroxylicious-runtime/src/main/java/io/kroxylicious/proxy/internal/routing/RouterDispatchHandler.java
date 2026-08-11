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

import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.CorrelationIdAllocator;
import io.kroxylicious.proxy.internal.CorrelationIdSpace;
import io.kroxylicious.proxy.internal.InternalRequestFrame;
import io.kroxylicious.proxy.internal.InternalResponseFrame;
import io.kroxylicious.proxy.internal.KafkaProxyExceptionMapper;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.util.NettyFutures.logFailure;

/**
 * Sits at the end of the VC-level filter chain (instead of
 * {@link io.kroxylicious.proxy.internal.FilterChainCompletionHandler}) when a
 * virtual cluster uses a router.
 *
 * <p>Statically-routed requests are forwarded directly to the
 * {@link ClientConnectionStateMachine}. Dynamically-routed requests are
 * deserialised and dispatched to {@link Router#onRequest}.
 *
 * <p>The {@link #write} override applies node ID translation for statically-routed
 * API keys whose responses carry broker node IDs.
 */
public class RouterDispatchHandler extends ChannelDuplexHandler implements RouterDispatch {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterDispatchHandler.class);

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

    private final Router router;
    final Map<String, RouteDescriptor> routes;
    private final Map<ApiKeys, String> staticRoutes;
    private final ClientConnectionStateMachine ccsm;
    private final String virtualClusterName;
    final NodeIdMapping nodeIdMapping;
    private final Map<Integer, HostPort> routerNodeAddresses;

    /**
     * Tracks correlation IDs of in-flight statically-routed requests that need response
     * node ID translation. Entries are removed when the response arrives in {@link #write}.
     */
    private final Map<Integer, String> pendingRoutes = new HashMap<>();

    final Map<Integer, PendingResponse> pendingResponses = new HashMap<>();

    private final CorrelationIdAllocator correlationIdAllocator = CorrelationIdSpace.createRouterAllocator();

    @Nullable
    private ResponseSequencer responseSequencer;

    @Nullable
    private ChannelHandlerContext ctx;

    @Nullable
    private final Integer nodeId;

    record PendingResponse(CompletableFuture<ApiMessage> future, String route, NodeIdMapping nodeIdMapping) {}

    /**
     * Creates a dispatch handler with a private (non-shared) upstream node address cache.
     *
     * @param router the router plugin instance that dynamic requests are dispatched to
     * @param routes the resolved routes of the router, keyed by route name
     * @param staticRoutes API keys that bypass the router, mapped to the route name they are forwarded on
     * @param ccsm the state machine for the client connection
     * @param virtualClusterName the name of the virtual cluster (used for logging)
     * @param nodeIdMapping the mapping between upstream and virtual node IDs
     * @param nodeId the virtual node ID targeted by this connection, or null for bootstrap connections
     */
    public RouterDispatchHandler(Router router,
                                 Map<String, RouteDescriptor> routes,
                                 Map<ApiKeys, String> staticRoutes,
                                 ClientConnectionStateMachine ccsm,
                                 String virtualClusterName,
                                 NodeIdMapping nodeIdMapping,
                                 @Nullable Integer nodeId) {
        this(router, routes, staticRoutes, new HashMap<>(), ccsm, virtualClusterName, nodeIdMapping, nodeId);
    }

    /**
     * Creates a dispatch handler using the supplied (possibly shared) upstream node address cache.
     *
     * @param router the router plugin instance that dynamic requests are dispatched to
     * @param routes the resolved routes of the router, keyed by route name
     * @param staticRoutes API keys that bypass the router, mapped to the route name they are forwarded on
     * @param sharedNodeAddresses cache of upstream node addresses keyed by virtual node ID
     * @param ccsm the state machine for the client connection
     * @param virtualClusterName the name of the virtual cluster (used for logging)
     * @param nodeIdMapping the mapping between upstream and virtual node IDs
     * @param nodeId the virtual node ID targeted by this connection, or null for bootstrap connections
     */
    public RouterDispatchHandler(Router router,
                                 Map<String, RouteDescriptor> routes,
                                 Map<ApiKeys, String> staticRoutes,
                                 Map<Integer, HostPort> sharedNodeAddresses,
                                 ClientConnectionStateMachine ccsm,
                                 String virtualClusterName,
                                 NodeIdMapping nodeIdMapping,
                                 @Nullable Integer nodeId) {
        this.router = router;
        this.routes = routes;
        this.staticRoutes = staticRoutes;
        this.routerNodeAddresses = sharedNodeAddresses;
        this.ccsm = ccsm;
        this.virtualClusterName = virtualClusterName;
        this.nodeIdMapping = nodeIdMapping;
        this.nodeId = nodeId;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Completes all pending router response futures exceptionally before closing the
     * router. This handles any case where the connection closes with outstanding
     * requests (forwarding failure, backend crash, drain timeout, etc.).
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        int abandoned = pendingResponses.size();
        if (abandoned > 0) {
            var cause = new IllegalStateException("Connection closed with " + abandoned + " pending router response(s)");
            for (var entry : pendingResponses.values()) {
                entry.future().completeExceptionally(cause);
            }
            pendingResponses.clear();
            LOGGER.atWarn()
                    .addKeyValue("virtualCluster", virtualClusterName)
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("abandonedResponses", abandoned)
                    .log("Connection closed with pending router responses");
        }
        router.close();
    }

    @Override
    public Map<String, RouteDescriptor> routes() {
        return routes;
    }

    @Override
    public NodeIdMapping nodeIdMapping() {
        return nodeIdMapping;
    }

    public CorrelationIdAllocator correlationIdAllocator() {
        return correlationIdAllocator;
    }

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

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestFrame frame) {
            ApiKeys apiKey = ApiKeys.forId(frame.apiKeyId());
            String staticRoute = staticRoutes.get(apiKey);
            if (staticRoute != null) {
                if (NODE_ID_TRANSLATION_APIS.contains(apiKey)) {
                    pendingRoutes.put(frame.correlationId(), staticRoute);
                }
                ((Frame) msg).setRouteName(staticRoute);
                ctx.fireChannelRead(msg);
                LOGGER.atTrace()
                        .addKeyValue("virtualCluster", virtualClusterName)
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("apiKey", apiKey)
                        .addKeyValue("route", staticRoute)
                        .addKeyValue("routingMode", "static")
                        .log("Request forwarded via static route");
                return;
            }
            if (msg instanceof DecodedRequestFrame<?> decoded) {
                dispatchDynamically(ctx, decoded);
                return;
            }
            LOGGER.atWarn()
                    .addKeyValue("virtualCluster", virtualClusterName)
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .log("Dynamically-routed API key arrived as opaque frame, forwarding via pipeline");
            ctx.fireChannelRead(msg);
            return;
        }
        ctx.fireChannelRead(msg);
    }

    private void fireChannelRead(Object msg) {
        Objects.requireNonNull(ctx, "fireChannelRead called before handlerAdded").fireChannelRead(msg);
    }

    private void dispatchDynamically(ChannelHandlerContext ctx, DecodedRequestFrame<?> frame) {
        ApiKeys apiKey = frame.apiKey();
        short apiVersion = frame.apiVersion();
        int correlationId = frame.correlationId();

        LOGGER.atTrace()
                .addKeyValue("virtualCluster", virtualClusterName)
                .addKeyValue("sessionId", ccsm.sessionId())
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("clientCorrelationId", correlationId)
                .addKeyValue("routingMode", "dynamic")
                .log("Dispatching request to router");

        if (responseSequencer == null) {
            responseSequencer = new ResponseSequencer(ctx.channel());
        }

        long sequence = responseSequencer.allocateSequence();
        var routingContext = new RouterContextImpl(
                frame,
                this,
                ccsm.sessionId(),
                ccsm.authenticatedSubject(),
                nodeId);

        if (frame instanceof InternalRequestFrame<?> oobFrame) {
            // Skip the slot immediately: OOB response bypasses the sequencer to avoid
            // deadlock when the OOB fires inside the onRequest/onResponse of a sequenced frame.
            responseSequencer.skip(sequence);
            router.onRequest(apiKey, apiVersion, frame.header(), frame.body(), routingContext)
                    .whenComplete((result, error) -> handleOobCompletion(ctx, oobFrame, result, error, apiKey, apiVersion, correlationId));
        }
        else {
            router.onRequest(apiKey, apiVersion, frame.header(), frame.body(), routingContext)
                    .whenComplete((result, error) -> handleRegularCompletion(ctx, result, error, apiKey, apiVersion, correlationId, sequence));
        }
    }

    private void handleOobCompletion(ChannelHandlerContext ctx, InternalRequestFrame<?> oobFrame,
                                     RouterResponse result, Throwable error,
                                     ApiKeys apiKey, short apiVersion, int correlationId) {
        try {
            if (error != null) {
                LOGGER.atError()
                        .addKeyValue("virtualCluster", virtualClusterName)
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("apiKey", apiKey)
                        .addKeyValue("clientCorrelationId", correlationId)
                        .setCause(error)
                        .log("Router returned failed future");
                oobFrame.promise().completeExceptionally(error);
                ctx.channel().close().addListener(logFailure(LOGGER, "close after router returned failed future for OOB request"));
                return;
            }
            if (!(result instanceof RouterResponseImpl rri)) {
                var cause = new IllegalStateException(
                        "Router returned unrecognised RouterResponse type (apiKey=" + apiKey + ", type=" + (result == null ? "null" : result.getClass().getName()) + ")");
                LOGGER.atError()
                        .addKeyValue("virtualCluster", virtualClusterName)
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("apiKey", apiKey)
                        .addKeyValue("resultType", result == null ? "null" : result.getClass().getName())
                        .log("Router returned unrecognised RouterResponse type; closing connection");
                oobFrame.promise().completeExceptionally(cause);
                ctx.channel().close().addListener(logFailure(LOGGER, "close after unrecognised router response type for OOB request"));
                return;
            }
            if (rri instanceof RouterResponseImpl.RespondWith rw) {
                writeOobResponse(ctx, rw, oobFrame, apiVersion, correlationId);
            }
            else {
                Throwable cause = rri instanceof RouterResponseImpl.RespondWithError rwe
                        ? rwe.exception()
                        : new IllegalStateException("Router returned no-reply response for OOB request (apiKey=" + apiKey + ")");
                oobFrame.promise().completeExceptionally(cause);
                if (rri.closeConnection()) {
                    ctx.channel().close().addListener(logFailure(LOGGER, "close requested by router for OOB request"));
                }
            }
        }
        finally {
            ccsm.onRoutedRequestComplete();
        }
    }

    private void writeOobResponse(ChannelHandlerContext ctx, RouterResponseImpl.RespondWith rw,
                                  InternalRequestFrame<?> oobFrame, short apiVersion, int correlationId) {
        var header = rw.header() != null ? rw.header() : new ResponseHeaderData();
        header.setCorrelationId(correlationId);
        var internalResponse = new InternalResponseFrame<>(
                oobFrame.recipient(), apiVersion, correlationId, header, rw.body(), oobFrame.promise());
        internalResponse.setRouteName(oobFrame.routeName());
        ctx.channel().writeAndFlush(internalResponse).addListener(f -> {
            if (!f.isSuccess()) {
                oobFrame.promise().completeExceptionally(f.cause());
            }
        });
    }

    private void handleRegularCompletion(ChannelHandlerContext ctx, RouterResponse result, Throwable error,
                                         ApiKeys apiKey, short apiVersion, int correlationId, long sequence) {
        if (error != null) {
            LOGGER.atError()
                    .addKeyValue("virtualCluster", virtualClusterName)
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("clientCorrelationId", correlationId)
                    .setCause(error)
                    .log("Router returned failed future");
            Objects.requireNonNull(responseSequencer).skip(sequence);
            ctx.channel().close().addListener(logFailure(LOGGER, "close after router returned failed future"));
            return;
        }
        if (!(result instanceof RouterResponseImpl rri)) {
            LOGGER.atError()
                    .addKeyValue("virtualCluster", virtualClusterName)
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("resultType", result == null ? "null" : result.getClass().getName())
                    .log("Router returned unrecognised RouterResponse type; closing connection");
            Objects.requireNonNull(responseSequencer).skip(sequence);
            ctx.channel().close().addListener(logFailure(LOGGER, "close after unrecognised router response type"));
            return;
        }
        deliverResponse(ctx, rri, apiKey, apiVersion, correlationId, sequence);
    }

    private void deliverResponse(ChannelHandlerContext ctx,
                                 RouterResponseImpl rri,
                                 ApiKeys apiKey,
                                 short apiVersion,
                                 int correlationId,
                                 long sequence) {
        switch (rri) {
            case RouterResponseImpl.RespondWith rw -> {
                ResponseHeaderData header = rw.header() != null ? rw.header() : new ResponseHeaderData();
                header.setCorrelationId(correlationId);
                var responseFrame = new DecodedResponseFrame<>(apiVersion, correlationId, header, rw.body());
                Objects.requireNonNull(responseSequencer).submit(sequence, responseFrame);
            }
            case RouterResponseImpl.RespondWithError rwe -> {
                AbstractResponse errorResponse = KafkaProxyExceptionMapper.errorResponseForMessage(
                        rwe.requestHeader(), rwe.request(), rwe.exception());
                ResponseHeaderData header = new ResponseHeaderData();
                header.setCorrelationId(correlationId);
                var responseFrame = new DecodedResponseFrame<>(apiVersion, correlationId, header, errorResponse.data());
                Objects.requireNonNull(responseSequencer).submit(sequence, responseFrame);
            }
            case RouterResponseImpl.RespondWithoutReply ignored -> {
                LOGGER.atTrace()
                        .addKeyValue("virtualCluster", virtualClusterName)
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("apiKey", apiKey)
                        .addKeyValue("clientCorrelationId", correlationId)
                        .log("Router completed request with no reply");
                Objects.requireNonNull(responseSequencer).skip(sequence);
            }
        }
        if (rri.closeConnection()) {
            ctx.channel().close().addListener(logFailure(LOGGER, "close requested by router after response delivery"));
        }
        ccsm.onRoutedRequestComplete();
    }

    // FutureReturnValueIgnored: `promise` is supplied by the caller and is notified with
    // the outcome of the write, so the returned future carries no additional information.
    @SuppressWarnings("FutureReturnValueIgnored")
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            int correlationId = frame.correlationId();
            if (correlationIdAllocator.inRange(correlationId)) {
                PendingResponse pendingResponse = pendingResponses.remove(correlationId);
                if (pendingResponse != null) {
                    NodeIdResponseTranslator.translate(frame.body(), frame.apiVersion(), pendingResponse.nodeIdMapping(), pendingResponse.route());
                    cacheNodeAddressesIfMetadata(frame.body());
                    pendingResponse.future().complete(frame.body());
                    frame.release();
                    LOGGER.atTrace()
                            .addKeyValue("virtualCluster", virtualClusterName)
                            .addKeyValue("sessionId", ccsm.sessionId())
                            .addKeyValue("routingCorrelationId", correlationId)
                            .log("Routed response matched to pending request");
                }
                else {
                    LOGGER.atWarn()
                            .addKeyValue("virtualCluster", virtualClusterName)
                            .addKeyValue("sessionId", ccsm.sessionId())
                            .addKeyValue("routingCorrelationId", correlationId)
                            .log("Received response with no pending routing future");
                    frame.release();
                    ctx.channel().close().addListener(logFailure(LOGGER, "close after response with no pending routing future"));
                }
                promise.setSuccess();
                return;
            }
            String routeName = pendingRoutes.remove(correlationId);
            if (routeName != null) {
                NodeIdResponseTranslator.translate(frame.body(), frame.apiVersion(), nodeIdMapping, routeName);
            }
        }
        ctx.write(msg, promise);
    }

    @Override
    public CompletionStage<ApiMessage> sendToAnyNode(String route,
                                                     RequestHeaderData header,
                                                     ApiMessage request,
                                                     String sessionId,
                                                     int clientCorrelationId) {
        return executeOnEventLoop(() -> doSendToAny(route, header, request, sessionId, clientCorrelationId));
    }

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
            catch (Exception e) {
                bridge.completeExceptionally(e);
            }
        });
        return bridge;
    }

    private CompletableFuture<ApiMessage> doSendToAny(String route, RequestHeaderData header, ApiMessage request, String sessionId,
                                                      int clientCorrelationId) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            withSendContext(LOGGER.atWarn(), virtualClusterName, sessionId, route, clientCorrelationId)
                    .log("Router attempted to send to unknown route");
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown route: " + route));
        }
        short requestApiVersion = header.requestApiVersion();
        int routingCorrelationId = correlationIdAllocator.allocateId();
        var frame = new DecodedRequestFrame<>(requestApiVersion, routingCorrelationId, true, header, request);

        frame.setRouteName(route);

        if (!frame.hasResponse()) {
            fireChannelRead(frame);
            withSendContext(LOGGER.atTrace(), virtualClusterName, sessionId, route, clientCorrelationId)
                    .addKeyValue("routingCorrelationId", routingCorrelationId)
                    .log("Fire-and-forget request sent to route (no response expected)");
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        pendingResponses.put(routingCorrelationId, new PendingResponse(future, route, nodeIdMapping));
        fireChannelRead(frame);

        withSendContext(LOGGER.atTrace(), virtualClusterName, sessionId, route, clientCorrelationId)
                .addKeyValue("routingCorrelationId", routingCorrelationId)
                .addKeyValue("apiVersion", requestApiVersion)
                .log("Request sent to route");
        return future;
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

    private CompletableFuture<ApiMessage> doSendToSpecificNode(int targetNodeId,
                                                               String route,
                                                               RequestHeaderData header,
                                                               ApiMessage request,
                                                               String sessionId,
                                                               int clientCorrelationId) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            withNodeContext(LOGGER.atWarn(), virtualClusterName, sessionId, route, targetNodeId)
                    .log("Target node resolved to unknown route");
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node " + targetNodeId + " resolved to unknown route: " + route));
        }

        short requestApiVersion = header.requestApiVersion();
        int routingCorrelationId = correlationIdAllocator.allocateId();
        var frame = new DecodedRequestFrame<>(requestApiVersion, routingCorrelationId, true, header, request);
        frame.setRouteName(route);
        if (rd.targetsCluster()) {
            frame.setTargetVirtualNodeId(targetNodeId);
        }

        if (!frame.hasResponse()) {
            fireChannelRead(frame);
            withSendContext(LOGGER.atTrace(), virtualClusterName, sessionId, route, clientCorrelationId)
                    .addKeyValue("targetNodeId", targetNodeId)
                    .addKeyValue("routingCorrelationId", routingCorrelationId)
                    .log("Fire-and-forget request sent to specific node (no response expected)");
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        pendingResponses.put(routingCorrelationId, new PendingResponse(future, route, nodeIdMapping));
        fireChannelRead(frame);

        withSendContext(LOGGER.atTrace(), virtualClusterName, sessionId, route, clientCorrelationId)
                .addKeyValue("targetNodeId", targetNodeId)
                .addKeyValue("routingCorrelationId", routingCorrelationId)
                .log("Request sent to specific node");
        return future;
    }

    private static LoggingEventBuilder withSendContext(LoggingEventBuilder event, String virtualClusterName, String sessionId, String route, int clientCorrelationId) {
        return event.addKeyValue("virtualCluster", virtualClusterName)
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("route", route)
                .addKeyValue("clientCorrelationId", clientCorrelationId);
    }

    private static LoggingEventBuilder withNodeContext(LoggingEventBuilder event, String virtualClusterName, String sessionId, String route, int targetNodeId) {
        return event.addKeyValue("virtualCluster", virtualClusterName)
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("targetNodeId", targetNodeId)
                .addKeyValue("route", route);
    }

    private void cacheNodeAddressesIfMetadata(Object body) {
        if (body instanceof MetadataResponseData md) {
            for (var broker : md.brokers()) {
                routerNodeAddresses.put(broker.nodeId(), new HostPort(broker.host(), broker.port()));
            }
            if (!md.brokers().isEmpty()) {
                LOGGER.atDebug()
                        .addKeyValue("virtualCluster", virtualClusterName)
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("brokerCount", md.brokers().size())
                        .log("Cached upstream node addresses from internal METADATA response");
            }
        }
    }
}
