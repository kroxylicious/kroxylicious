/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
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
 * A Netty handler that wraps a single {@link Router} — either the top-level
 * router at the end of the virtual-cluster filter chain, or a nested router
 * sitting between outer-route and inner-route filters. Intercepts inbound
 * request frames, decides whether each is statically or dynamically routed,
 * invokes the {@link Router} for dynamic requests, and delivers the resulting
 * response to the client (top-level, via a {@link ResponseSequencer}) or
 * upstream through outer-route filters (nested, via {@code ctx.write}).
 *
 * <p>Frame creation, correlation-ID tracking, and response matching are
 * delegated to a {@link RouteDispatcher}. The handler owns the policy
 * decisions that differ between top-level and nested use: response sequencing,
 * connection-close semantics, out-of-band request handling, and
 * {@link ClientConnectionStateMachine} interaction.
 *
 * <p>Use the {@link #topLevel} and {@link #nested} factory methods to create
 * instances. Not thread-safe; all callers must be on the same Netty event loop.
 */
public class RoutingHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingHandler.class);

    // --- Configuration ---

    private final RouteDispatcher dispatcher;
    private final String virtualClusterName;
    private final String sessionId;
    private final Subject subject;
    @Nullable
    private final Integer nodeId;

    @Nullable
    private final String activationRoute;

    @Nullable
    private final ClientConnectionStateMachine ccsm;

    // --- Router lifecycle ---

    @Nullable
    private Router router;
    @Nullable
    private Map<ApiKeys, String> resolvedStaticRoutes;
    @Nullable
    private final RouterChainFactory routerChainFactory;
    @Nullable
    private final String routerName;

    // --- Sequencing (top-level only) ---

    @Nullable
    private ResponseSequencer responseSequencer;

    @Nullable
    private ChannelHandlerContext ctx;

    // all parameters are genuinely needed: dispatch, identity, activation, lifecycle, auth
    @SuppressWarnings("java:S107")
    private RoutingHandler(RouteDispatcher dispatcher,
                           String virtualClusterName,
                           String sessionId,
                           Subject subject,
                           @Nullable Integer nodeId,
                           @Nullable String activationRoute,
                           @Nullable ClientConnectionStateMachine ccsm,
                           @Nullable Router router,
                           @Nullable Map<ApiKeys, String> staticRoutes,
                           @Nullable RouterChainFactory routerChainFactory,
                           @Nullable String routerName) {
        this.dispatcher = dispatcher;
        this.virtualClusterName = virtualClusterName;
        this.sessionId = sessionId;
        this.subject = subject;
        this.nodeId = nodeId;
        this.activationRoute = activationRoute;
        this.ccsm = ccsm;
        this.router = router;
        this.resolvedStaticRoutes = staticRoutes;
        this.routerChainFactory = routerChainFactory;
        this.routerName = routerName;
    }

    /**
     * Creates a top-level routing handler that sits at the end of the VC-level filter chain.
     * Uses a {@link ResponseSequencer} for response ordering and interacts with
     * {@link ClientConnectionStateMachine} for connection lifecycle.
     */
    public static RoutingHandler topLevel(Router router,
                                          Map<String, RouteDescriptor> routes,
                                          Map<ApiKeys, String> staticRoutes,
                                          Map<Integer, HostPort> sharedNodeAddresses,
                                          ClientConnectionStateMachine ccsm,
                                          String virtualClusterName,
                                          NodeIdMapping nodeIdMapping,
                                          @Nullable Integer nodeId) {
        var allocator = CorrelationIdSpace.createRouterAllocator();
        var dispatcher = new RouteDispatcher(routes, nodeIdMapping, "", allocator, sharedNodeAddresses, virtualClusterName);
        return new RoutingHandler(dispatcher, virtualClusterName,
                ccsm.sessionId(), ccsm.authenticatedSubject(), nodeId,
                null, ccsm,
                router, staticRoutes,
                null, null);
    }

    /**
     * Creates a nested routing handler that intercepts frames matching the given
     * activation route. No response sequencing — the outer level handles ordering.
     * Ignores router close-connection requests.
     */
    // all parameters are genuinely needed: identity, routing config, protocol infrastructure, session, auth, network
    @SuppressWarnings("java:S107")
    public static RoutingHandler nested(String activationRoute,
                                        String nestedRouterName,
                                        String virtualClusterName,
                                        RouterChainFactory routerChainFactory,
                                        Map<String, RouteDescriptor> nestedRoutes,
                                        NodeIdMapping nestedNodeIdMapping,
                                        CorrelationIdAllocator correlationIdAllocator,
                                        Map<Integer, HostPort> routerNodeAddresses,
                                        String sessionId,
                                        Subject subject,
                                        @Nullable Integer nodeId) {
        String routePrefix = nestedRouterName + "/";
        var dispatcher = new RouteDispatcher(nestedRoutes, nestedNodeIdMapping, routePrefix,
                correlationIdAllocator, routerNodeAddresses, virtualClusterName);
        return new RoutingHandler(dispatcher, virtualClusterName,
                sessionId, subject, nodeId,
                activationRoute, null,
                null, null,
                routerChainFactory, nestedRouterName);
    }

    // --- Accessors ---

    RouteDispatcher dispatcher() {
        return dispatcher;
    }

    public CorrelationIdAllocator correlationIdAllocator() {
        return dispatcher.correlationIdAllocator();
    }

    public Map<Integer, HostPort> routerNodeAddresses() {
        return dispatcher.routerNodeAddresses();
    }

    public Optional<HostPort> resolveRouterNodeAddress(int virtualNodeId) {
        return dispatcher.resolveRouterNodeAddress(virtualNodeId);
    }

    // --- Netty lifecycle ---

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        dispatcher.setContext(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        dispatcher.failAllPending(sessionId);
        if (router != null) {
            router.close();
            router = null;
        }
    }

    // --- Inbound (requests) ---

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof RequestFrame frame)) {
            ctx.fireChannelRead(msg);
            return;
        }

        if (activationRoute != null && !activationRoute.equals(frame.routeName())) {
            ctx.fireChannelRead(msg);
            return;
        }

        ensureRouterCreated();

        ApiKeys apiKey = ApiKeys.forId(frame.apiKeyId());
        String staticRoute = resolvedStaticRoutes.get(apiKey);
        if (staticRoute != null) {
            dispatchStaticRoute(ctx, frame, msg, apiKey, staticRoute);
            return;
        }

        if (msg instanceof DecodedRequestFrame<?> decoded) {
            dispatchDynamically(ctx, decoded);
            return;
        }

        handleOpaqueFrame(ctx, frame, apiKey, msg);
    }

    private void dispatchStaticRoute(ChannelHandlerContext ctx, RequestFrame frame, Object msg, ApiKeys apiKey, String staticRoute) {
        String qualifiedRoute = dispatcher.qualifyRoute(staticRoute);
        if (RouteDispatcher.NODE_ID_TRANSLATION_APIS.contains(apiKey)) {
            dispatcher.trackStaticRoute(frame.correlationId(), staticRoute);
        }
        ((Frame) msg).setRouteName(qualifiedRoute);
        ctx.fireChannelRead(msg);
        LOGGER.atTrace()
                .addKeyValue("virtualCluster", virtualClusterName)
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("route", qualifiedRoute)
                .addKeyValue("routingMode", "static")
                .log("Request forwarded via static route");
    }

    private void handleOpaqueFrame(ChannelHandlerContext ctx, RequestFrame frame, ApiKeys apiKey, Object msg) {
        if (activationRoute != null) {
            LOGGER.atWarn()
                    .addKeyValue("virtualCluster", virtualClusterName)
                    .addKeyValue("sessionId", sessionId)
                    .addKeyValue("apiKey", apiKey)
                    .log("Opaque frame arrived for dynamically-routed nested API key; decode predicate misconfigured");
            ctx.close();
        }
        else {
            LOGGER.atWarn()
                    .addKeyValue("virtualCluster", virtualClusterName)
                    .addKeyValue("sessionId", sessionId)
                    .addKeyValue("apiKey", apiKey)
                    .log("Dynamically-routed API key arrived as opaque frame, forwarding via pipeline");
            ctx.fireChannelRead(msg);
        }
    }

    // --- Dynamic dispatch ---

    private void dispatchDynamically(ChannelHandlerContext ctx, DecodedRequestFrame<?> frame) {
        ApiKeys apiKey = frame.apiKey();
        short apiVersion = frame.apiVersion();
        int correlationId = frame.correlationId();

        LOGGER.atTrace()
                .addKeyValue("virtualCluster", virtualClusterName)
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("clientCorrelationId", correlationId)
                .addKeyValue("routingMode", "dynamic")
                .log("Dispatching request to router");

        long sequence = -1;
        if (ccsm != null) {
            if (responseSequencer == null) {
                responseSequencer = new ResponseSequencer(ctx.channel());
            }
            sequence = responseSequencer.allocateSequence();
        }

        var routingContext = new RouterContextImpl(frame, dispatcher, sessionId, subject, nodeId);

        if (ccsm != null && frame instanceof InternalRequestFrame<?> oobFrame) {
            Objects.requireNonNull(responseSequencer).skip(sequence);
            router.onRequest(apiKey, apiVersion, frame.header(), frame.body(), routingContext)
                    .whenComplete((result, error) -> handleOobCompletion(ctx, oobFrame, result, error, apiKey, apiVersion, correlationId));
        }
        else {
            long seq = sequence;
            router.onRequest(apiKey, apiVersion, frame.header(), frame.body(), routingContext)
                    .whenComplete((result, error) -> handleCompletion(ctx, frame, result, error, apiKey, apiVersion, correlationId, seq));
        }
    }

    // --- OOB completion (top-level only) ---

    private void handleOobCompletion(ChannelHandlerContext ctx, InternalRequestFrame<?> oobFrame,
                                     RouterResponse result, Throwable error,
                                     ApiKeys apiKey, short apiVersion, int correlationId) {
        try {
            if (error != null) {
                LOGGER.atError()
                        .addKeyValue("virtualCluster", virtualClusterName)
                        .addKeyValue("sessionId", sessionId)
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
                        .addKeyValue("sessionId", sessionId)
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
            Objects.requireNonNull(ccsm).onRoutedRequestComplete();
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

    // --- Regular completion (both levels) ---

    private void handleCompletion(ChannelHandlerContext ctx, DecodedRequestFrame<?> requestFrame,
                                  RouterResponse result, Throwable error,
                                  ApiKeys apiKey, short apiVersion, int correlationId, long sequence) {
        if (error != null) {
            LOGGER.atError()
                    .addKeyValue("virtualCluster", virtualClusterName)
                    .addKeyValue("sessionId", sessionId)
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("clientCorrelationId", correlationId)
                    .setCause(error)
                    .log("Router returned failed future");
            if (ccsm != null) {
                Objects.requireNonNull(responseSequencer).skip(sequence);
                ctx.channel().close().addListener(logFailure(LOGGER, "close after router returned failed future"));
            }
            else {
                writeErrorResponseUpstream(ctx, requestFrame, error);
            }
            notifyRequestComplete();
            return;
        }
        if (!(result instanceof RouterResponseImpl rri)) {
            LOGGER.atError()
                    .addKeyValue("virtualCluster", virtualClusterName)
                    .addKeyValue("sessionId", sessionId)
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("resultType", result == null ? "null" : result.getClass().getName())
                    .log("Router returned unrecognised RouterResponse type; closing connection");
            if (ccsm != null) {
                Objects.requireNonNull(responseSequencer).skip(sequence);
                ctx.channel().close().addListener(logFailure(LOGGER, "close after unrecognised router response type"));
            }
            else {
                writeErrorResponseUpstream(ctx, requestFrame,
                        new IllegalStateException("Router returned unrecognised response type"));
            }
            notifyRequestComplete();
            return;
        }

        if (rri.closeConnection()) {
            if (ccsm != null) {
                ctx.channel().close().addListener(logFailure(LOGGER, "close requested by router after response delivery"));
            }
            else {
                LOGGER.atWarn()
                        .addKeyValue("virtualCluster", virtualClusterName)
                        .addKeyValue("sessionId", sessionId)
                        .log("Nested router attempted to close connection; ignoring close request");
            }
        }

        deliverResponse(ctx, requestFrame, rri, apiKey, apiVersion, correlationId, sequence);
        notifyRequestComplete();
    }

    private void deliverResponse(ChannelHandlerContext ctx,
                                 DecodedRequestFrame<?> requestFrame,
                                 RouterResponseImpl rri,
                                 ApiKeys apiKey,
                                 short apiVersion,
                                 int correlationId,
                                 long sequence) {
        switch (rri) {
            case RouterResponseImpl.RespondWith rw -> {
                ResponseHeaderData header = rw.header() != null ? rw.header() : new ResponseHeaderData();
                header.setCorrelationId(correlationId);
                ApiMessage body = rw.body();
                deliverResponseFrame(ctx, requestFrame, apiVersion, correlationId, header, body, sequence);
            }
            case RouterResponseImpl.RespondWithError rwe -> {
                AbstractResponse errorResponse = KafkaProxyExceptionMapper.errorResponseForMessage(
                        rwe.requestHeader(), rwe.request(), rwe.exception());
                ResponseHeaderData header = new ResponseHeaderData();
                header.setCorrelationId(correlationId);
                deliverResponseFrame(ctx, requestFrame, apiVersion, correlationId, header, errorResponse.data(), sequence);
            }
            case RouterResponseImpl.RespondWithoutReply ignored -> {
                LOGGER.atTrace()
                        .addKeyValue("virtualCluster", virtualClusterName)
                        .addKeyValue("sessionId", sessionId)
                        .addKeyValue("apiKey", apiKey)
                        .addKeyValue("clientCorrelationId", correlationId)
                        .log("Router completed request with no reply");
                if (responseSequencer != null) {
                    responseSequencer.skip(sequence);
                }
            }
        }
    }

    private void deliverResponseFrame(ChannelHandlerContext ctx,
                                      DecodedRequestFrame<?> requestFrame,
                                      short apiVersion,
                                      int correlationId,
                                      ResponseHeaderData header,
                                      ApiMessage body,
                                      long sequence) {
        var responseFrame = new DecodedResponseFrame<>(apiVersion, correlationId, header, body);
        if (responseSequencer != null) {
            responseSequencer.submit(sequence, responseFrame);
        }
        else {
            if (activationRoute != null) {
                responseFrame.setRouteName(activationRoute);
            }
            ctx.write(responseFrame, ctx.voidPromise());
            ctx.flush();
        }
    }

    private void writeErrorResponseUpstream(ChannelHandlerContext ctx,
                                            DecodedRequestFrame<?> requestFrame,
                                            Throwable error) {
        var header = new ResponseHeaderData();
        header.setCorrelationId(requestFrame.correlationId());
        ApiMessage body = KafkaProxyExceptionMapper.errorResponseForMessage(
                requestFrame.header(), requestFrame.body(), new UnknownServerException(error.getMessage())).data();
        var responseFrame = new DecodedResponseFrame<>(requestFrame.apiVersion(), requestFrame.correlationId(), header, body);
        if (activationRoute != null) {
            responseFrame.setRouteName(activationRoute);
        }
        ctx.write(responseFrame, ctx.voidPromise());
        ctx.flush();
    }

    private void notifyRequestComplete() {
        if (ccsm != null) {
            ccsm.onRoutedRequestComplete();
        }
    }

    // --- Outbound (responses) ---

    // FutureReturnValueIgnored: `promise` is supplied by the caller and is notified with
    // the outcome of the write, so the returned future carries no additional information.
    @SuppressWarnings("FutureReturnValueIgnored")
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            RouteDispatcher.ResponseOutcome outcome = dispatcher.handleResponse(frame, sessionId);
            if (outcome == RouteDispatcher.ResponseOutcome.CONSUMED) {
                promise.setSuccess();
                return;
            }
            if (outcome == RouteDispatcher.ResponseOutcome.UNHANDLED && ccsm != null
                    && dispatcher.correlationIdAllocator().inRange(frame.correlationId())) {
                LOGGER.atWarn()
                        .addKeyValue("virtualCluster", virtualClusterName)
                        .addKeyValue("sessionId", sessionId)
                        .addKeyValue("routingCorrelationId", frame.correlationId())
                        .log("Received response with no pending routing future");
                frame.release();
                ctx.channel().close().addListener(logFailure(LOGGER, "close after response with no pending routing future"));
                promise.setSuccess();
                return;
            }
        }
        ctx.write(msg, promise);
    }

    // --- Router lifecycle ---

    private void ensureRouterCreated() {
        if (router == null) {
            Objects.requireNonNull(routerChainFactory, "No router and no factory to create one");
            Objects.requireNonNull(routerName, "routerName required for lazy router creation");
            router = routerChainFactory.createRouter(routerName, virtualClusterName);
        }
        if (resolvedStaticRoutes == null) {
            resolvedStaticRoutes = router.staticRoutes();
        }
    }
}
