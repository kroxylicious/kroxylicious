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
import io.netty.util.concurrent.EventExecutor;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.CorrelationIdAllocator;
import io.kroxylicious.proxy.internal.CorrelationIdSpace;
import io.kroxylicious.proxy.internal.KafkaProxyExceptionMapper;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

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
public class RouterDispatchHandler extends ChannelDuplexHandler {

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
    final NodeIdMapping nodeIdMapping;
    private final Map<Integer, HostPort> routerNodeAddresses = new HashMap<>();

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
    private EventExecutor eventExecutor;

    @Nullable
    private final Integer nodeId;

    record PendingResponse(CompletableFuture<ApiMessage> future, String route) {}

    public RouterDispatchHandler(Router router,
                                 Map<String, RouteDescriptor> routes,
                                 Map<ApiKeys, String> staticRoutes,
                                 ClientConnectionStateMachine ccsm,
                                 NodeIdMapping nodeIdMapping,
                                 @Nullable Integer nodeId) {
        this.router = router;
        this.routes = routes;
        this.staticRoutes = staticRoutes;
        this.ccsm = ccsm;
        this.nodeIdMapping = nodeIdMapping;
        this.nodeId = nodeId;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.eventExecutor = ctx.executor();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        router.close();
    }

    /**
     * Returns the upstream address for the given virtual node ID, as learned from the most
     * recent internal METADATA response. Returns empty if the address has not been cached yet.
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
                ccsm.forwardToRoute(staticRoute, msg);
                LOGGER.atTrace()
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
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .log("Dynamically-routed API key arrived as opaque frame, forwarding to CCSM");
            ccsm.onClientFilterChainComplete(msg);
            return;
        }
        ccsm.onClientFilterChainComplete(msg);
    }

    private void dispatchDynamically(ChannelHandlerContext ctx, DecodedRequestFrame<?> frame) {
        ApiKeys apiKey = frame.apiKey();
        short apiVersion = frame.apiVersion();
        int correlationId = frame.correlationId();

        LOGGER.atTrace()
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

        router.onRequest(apiKey, apiVersion, frame.header(), frame.body(), routingContext)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        LOGGER.atError()
                                .addKeyValue("sessionId", ccsm.sessionId())
                                .addKeyValue("apiKey", apiKey)
                                .addKeyValue("clientCorrelationId", correlationId)
                                .setCause(error)
                                .log("Router returned failed future");
                        ctx.channel().close();
                        return;
                    }
                    if (!(result instanceof RouterResponseImpl rri)) {
                        LOGGER.atError()
                                .addKeyValue("sessionId", ccsm.sessionId())
                                .addKeyValue("apiKey", apiKey)
                                .addKeyValue("resultType", result == null ? "null" : result.getClass().getName())
                                .log("Router returned unrecognised RouterResponse type; closing connection");
                        ctx.channel().close();
                        return;
                    }
                    deliverResponse(ctx, rri, apiKey, apiVersion, correlationId, sequence);
                });
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
            case RouterResponseImpl.RespondWithoutReply ignored -> LOGGER.atTrace()
                    .addKeyValue("sessionId", ccsm.sessionId())
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("clientCorrelationId", correlationId)
                    .log("Router completed request with no reply");
        }
        if (rri.closeConnection()) {
            ctx.channel().close();
        }
        ccsm.onRoutedRequestComplete();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            int correlationId = frame.correlationId();
            if (correlationIdAllocator.inRange(correlationId)) {
                PendingResponse pendingResponse = pendingResponses.remove(correlationId);
                if (pendingResponse != null) {
                    NodeIdResponseTranslator.translate(frame.body(), frame.apiVersion(), nodeIdMapping, pendingResponse.route());
                    cacheNodeAddressesIfMetadata(frame.body());
                    pendingResponse.future().complete(frame.body());
                    LOGGER.atTrace()
                            .addKeyValue("sessionId", ccsm.sessionId())
                            .addKeyValue("routingCorrelationId", correlationId)
                            .log("Routed response matched to pending request");
                }
                else {
                    LOGGER.atWarn()
                            .addKeyValue("sessionId", ccsm.sessionId())
                            .addKeyValue("routingCorrelationId", correlationId)
                            .log("Received response with no pending routing future");
                    ctx.channel().close();
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

    CompletionStage<ApiMessage> sendToAnyNode(String route,
                                              RequestHeaderData header,
                                              ApiMessage request,
                                              String sessionId,
                                              int clientCorrelationId) {
        return executeOnEventLoop(() -> doSendToAny(route, header, request, sessionId, clientCorrelationId));
    }

    private <T> CompletionStage<T> executeOnEventLoop(Supplier<CompletableFuture<T>> work) {
        var executor = Objects.requireNonNull(eventExecutor, "sendRequest called before handlerAdded");
        if (executor.inEventLoop()) {
            return work.get();
        }
        CompletableFuture<T> bridge = new CompletableFuture<>();
        executor.execute(() -> work.get().whenComplete((r, e) -> {
            if (e != null) {
                bridge.completeExceptionally(e);
            }
            else {
                bridge.complete(r);
            }
        }));
        return bridge;
    }

    private CompletableFuture<ApiMessage> doSendToAny(String route, RequestHeaderData header, ApiMessage request, String sessionId,
                                                      int clientCorrelationId) {
        RouteDescriptor rd = routes.get(route);
        if (rd == null) {
            withSendContext(LOGGER.atWarn(), sessionId, route, clientCorrelationId)
                    .log("Router attempted to send to unknown route");
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown route: " + route));
        }
        if (!rd.targetsCluster()) {
            withSendContext(LOGGER.atWarn(), sessionId, route, clientCorrelationId)
                    .log("Router attempted unsupported nested router route");
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException("Routing to nested routers is not yet supported (route: " + route + ")"));
        }

        short requestApiVersion = header.requestApiVersion();
        int routingCorrelationId = correlationIdAllocator.allocateId();
        var frame = new DecodedRequestFrame<>(requestApiVersion, routingCorrelationId, true, header, request);

        if (!frame.hasResponse()) {
            ccsm.forwardToRoute(route, frame);
            withSendContext(LOGGER.atTrace(), sessionId, route, clientCorrelationId)
                    .addKeyValue("routingCorrelationId", routingCorrelationId)
                    .log("Fire-and-forget request sent to route (no response expected)");
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        pendingResponses.put(routingCorrelationId, new PendingResponse(future, route));

        ccsm.forwardToRoute(route, frame);
        withSendContext(LOGGER.atTrace(), sessionId, route, clientCorrelationId)
                .addKeyValue("routingCorrelationId", routingCorrelationId)
                .addKeyValue("apiVersion", requestApiVersion)
                .log("Request sent to route");
        return future;
    }

    CompletionStage<ApiMessage> sendToSpecificNode(int targetNodeId,
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
        if (rd == null || !rd.targetsCluster()) {
            withNodeContext(LOGGER.atWarn(), sessionId, route, targetNodeId)
                    .log("Target node resolved to invalid route");
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node " + targetNodeId + " resolved to invalid route: " + route));
        }

        short requestApiVersion = header.requestApiVersion();
        int routingCorrelationId = correlationIdAllocator.allocateId();
        var frame = new DecodedRequestFrame<>(requestApiVersion, routingCorrelationId, true, header, request);

        if (!frame.hasResponse()) {
            ccsm.forwardToNode(targetNodeId, route, frame);
            withSendContext(LOGGER.atTrace(), sessionId, route, clientCorrelationId)
                    .addKeyValue("targetNodeId", targetNodeId)
                    .addKeyValue("routingCorrelationId", routingCorrelationId)
                    .log("Fire-and-forget request sent to specific node (no response expected)");
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        pendingResponses.put(routingCorrelationId, new PendingResponse(future, route));

        try {
            ccsm.forwardToNode(targetNodeId, route, frame);
        }
        catch (Exception e) {
            pendingResponses.remove(routingCorrelationId);
            withNodeContext(LOGGER.atWarn(), sessionId, route, targetNodeId)
                    .setCause(LOGGER.isDebugEnabled() ? e : null)
                    .addKeyValue("error", e.getMessage())
                    .log(LOGGER.isDebugEnabled()
                            ? "Failed to forward request to node"
                            : "Failed to forward request to node, increase log level to DEBUG for stacktrace");
            return CompletableFuture.failedFuture(e);
        }

        withSendContext(LOGGER.atTrace(), sessionId, route, clientCorrelationId)
                .addKeyValue("targetNodeId", targetNodeId)
                .addKeyValue("routingCorrelationId", routingCorrelationId)
                .log("Request sent to specific node");
        return future;
    }

    private static LoggingEventBuilder withSendContext(LoggingEventBuilder event, String sessionId, String route, int clientCorrelationId) {
        return event.addKeyValue("sessionId", sessionId)
                .addKeyValue("route", route)
                .addKeyValue("clientCorrelationId", clientCorrelationId);
    }

    private static LoggingEventBuilder withNodeContext(LoggingEventBuilder event, String sessionId, String route, int targetNodeId) {
        return event.addKeyValue("sessionId", sessionId)
                .addKeyValue("targetNodeId", targetNodeId)
                .addKeyValue("route", route);
    }

    // post transformation of ids into virtual ids
    private void cacheNodeAddressesIfMetadata(Object body) {
        if (body instanceof MetadataResponseData md) {
            for (var broker : md.brokers()) {
                routerNodeAddresses.put(broker.nodeId(), new HostPort(broker.host(), broker.port()));
            }
            if (!md.brokers().isEmpty()) {
                LOGGER.atDebug()
                        .addKeyValue("sessionId", ccsm.sessionId())
                        .addKeyValue("brokerCount", md.brokers().size())
                        .log("Cached upstream node addresses from internal METADATA response");
            }
        }
    }
}
