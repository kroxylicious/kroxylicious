/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.kafka.common.errors.ApiException;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.CloseReason;
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
 * {@link ClientConnectionStateMachine} interaction. The {@link #requestSource}
 * field distinguishes the two modes without scattering null checks throughout
 * the class: a {@link VirtualClusterRequestSource} indicates top-level, a
 * {@link RouterRequestSource} indicates nested.
 *
 * <h2>Pipeline structure</h2>
 * <p>One handler of this type is installed per router level per connection.
 * A router whose routes target a mix of upstream clusters and nested routers
 * results in one {@code RoutingHandler} per router-targeting route, each
 * intercepting only frames whose route name matches its
 * {@link RouterRequestSource#activationRoute()}; cluster-targeting frames pass
 * through unchanged. Because each activation route has its own handler
 * instance and its own lazily-created {@link Router}, a router referenced from
 * multiple parent routes (a diamond in the routing DAG) is effectively
 * unrolled into a tree: each path to it gets independent handler and router
 * instances.
 *
 * <p>Use the {@link #topLevel} and {@link #nested} factory methods to create
 * instances. Not thread-safe; all callers must be on the same Netty event loop.
 */
public class RoutingHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingHandler.class);
    private static final String KOG_KEY_RESULT_TYPE = "resultType";
    private static final String LOG_KEY_CLIENT_CORRELATION_ID = "clientCorrelationId";
    private static final String LOG_KEY_API_KEY = "apiKey";
    private static final String LOG_KEY_SESSION_ID = "sessionId";
    private static final String LOG_KEY_VIRTUAL_CLUSTER = "virtualCluster";

    // --- Configuration ---

    private final RouteDispatcher dispatcher;
    private final String virtualClusterName;
    private final String sessionId;
    private final Subject subject;
    @Nullable
    private final Integer nodeId;

    private final RequestSource requestSource;

    // --- Router lifecycle ---

    @Nullable
    private Router router;
    @Nullable
    private Map<ApiKeys, String> resolvedStaticRoutes;

    // --- Sequencing (top-level only) ---

    @Nullable
    private ResponseSequencer responseSequencer;

    // all parameters are genuinely needed: dispatch, identity, request source, router state
    @SuppressWarnings("java:S107")
    private RoutingHandler(RouteDispatcher dispatcher,
                           String virtualClusterName,
                           String sessionId,
                           Subject subject,
                           @Nullable Integer nodeId,
                           RequestSource requestSource,
                           @Nullable Router router,
                           @Nullable Map<ApiKeys, String> staticRoutes) {
        this.dispatcher = dispatcher;
        this.virtualClusterName = virtualClusterName;
        this.sessionId = sessionId;
        this.subject = subject;
        this.nodeId = nodeId;
        this.requestSource = requestSource;
        this.router = router;
        this.resolvedStaticRoutes = staticRoutes;
    }

    /**
     * Creates a top-level routing handler that sits at the end of the VC-level filter chain.
     * Uses a {@link ResponseSequencer} for response ordering and interacts with
     * {@link ClientConnectionStateMachine} for connection lifecycle.
     *
     * @param router the router plugin instance for this connection
     * @param routes all route descriptors for this virtual cluster (top-level and nested, qualified names)
     * @param staticRoutes map from API key to the single route that always handles that key,
     *        used to bypass the router for requests that don't need dynamic routing
     * @param sharedNodeAddresses node addresses shared across routes (e.g. from a prior metadata response),
     *        used to route node-specific requests without an additional metadata round-trip
     * @param ccsm the connection state machine; provides session ID, subject, and connection lifecycle hooks
     * @param nodeIdMapping the virtual-to-target node ID mapping for the top-level routing level
     * @param nodeId the virtual node ID of the gateway port that accepted this connection,
     *        or {@code null} if the gateway does not identify a specific node
     * @return the top-level routing handler
     */
    public static RoutingHandler topLevel(Router router,
                                          Map<String, RouteDescriptor> routes,
                                          Map<ApiKeys, String> staticRoutes,
                                          Map<Integer, HostPort> sharedNodeAddresses,
                                          ClientConnectionStateMachine ccsm,
                                          NodeIdMapping nodeIdMapping,
                                          @Nullable Integer nodeId) {
        String virtualClusterName = ccsm.clusterName();
        var allocator = CorrelationIdSpace.createRouterAllocator();
        var dispatcher = new RouteDispatcher(routes, nodeIdMapping, "", allocator, sharedNodeAddresses, virtualClusterName);
        return new RoutingHandler(dispatcher, virtualClusterName,
                ccsm.sessionId(), ccsm.authenticatedSubject(), nodeId,
                new VirtualClusterRequestSource(ccsm),
                router, staticRoutes);
    }

    /**
     * Creates a nested routing handler that intercepts frames matching the given
     * activation route. No response sequencing — the outer level handles ordering.
     * Ignores router close-connection requests.
     *
     * @param activationRoute the qualified route name (e.g. {@code "outerRouter/routeName"})
     *        that this handler intercepts; frames with a different route name pass through
     * @param nestedRouterName the name of the nested router, used to build the route prefix
     *        ({@code "routerName/"}) for qualified route names at this level
     * @param virtualClusterName the virtual cluster name, used for logging
     * @param routerChainFactory factory for creating the nested router and its filter chain
     * @param nestedRoutes the route descriptors for this nested router level (qualified names)
     * @param nestedNodeIdMapping the virtual-to-target node ID mapping for this nested level
     * @param correlationIdAllocator allocates upstream correlation IDs, shared with the top-level
     *        handler so all in-flight requests on this connection use a single ID space
     * @param routerNodeAddresses node addresses known at this nesting level, populated from
     *        metadata responses received through this handler
     * @param sessionId the proxy session ID, used for logging and diagnostics
     * @param subject the authenticated subject for this connection
     * @param nodeId the virtual node ID passed from the enclosing routing level,
     *        or {@code null} if not available at this nesting depth
     * @return the nested routing handler
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
                new RouterRequestSource(activationRoute, routerChainFactory, nestedRouterName),
                null, null);
    }

    // --- Accessors ---

    RouteDispatcher dispatcher() {
        return dispatcher;
    }

    /**
     * Returns the correlation ID allocator shared by all routing levels on this connection.
     *
     * @return the correlation ID allocator
     */
    public CorrelationIdAllocator correlationIdAllocator() {
        return dispatcher.correlationIdAllocator();
    }

    /**
     * Returns the upstream node addresses known at this routing level, keyed by virtual node ID.
     *
     * @return the known upstream node addresses
     */
    public Map<Integer, HostPort> routerNodeAddresses() {
        return dispatcher.routerNodeAddresses();
    }

    /**
     * Returns the upstream address for the given virtual node ID, as learned from the most
     * recent internal METADATA response. Returns empty if the address has not been cached yet.
     *
     * @param virtualNodeId the virtual node ID to resolve
     * @return the upstream address of the node, or empty if not yet known
     */
    public Optional<HostPort> resolveRouterNodeAddress(int virtualNodeId) {
        return dispatcher.resolveRouterNodeAddress(virtualNodeId);
    }

    // --- Netty lifecycle ---

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
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

        if (requestSource instanceof RouterRequestSource rs && !rs.intercepts(frame)) {
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

        handleOpaqueFrame(ctx, apiKey, msg);
    }

    private void dispatchStaticRoute(ChannelHandlerContext ctx, RequestFrame frame, Object msg, ApiKeys apiKey, String staticRoute) {
        String qualifiedRoute = dispatcher.qualifyRoute(staticRoute);
        // OOB frames all share the reserved out-of-band correlation ID, so tracking one for node-ID
        // translation would collide with any other concurrently in-flight, statically-routed OOB request.
        if (!(msg instanceof InternalRequestFrame<?>) && RouteDispatcher.NODE_ID_TRANSLATION_APIS.contains(apiKey)) {
            dispatcher.trackStaticRoute(frame.correlationId(), staticRoute);
        }
        ((Frame) msg).setRouteName(qualifiedRoute);
        ctx.fireChannelRead(msg);
        LOGGER.atTrace()
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue(LOG_KEY_API_KEY, apiKey)
                .addKeyValue("route", qualifiedRoute)
                .addKeyValue("routingMode", "static")
                .log("Request forwarded via static route");
    }

    private void handleOpaqueFrame(ChannelHandlerContext ctx, ApiKeys apiKey, Object msg) {
        if (requestSource instanceof RouterRequestSource) {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_API_KEY, apiKey)
                    .log("Opaque frame arrived for dynamically-routed nested API key; decode predicate misconfigured");
            ctx.close().addListener(logFailure(LOGGER, "close after opaque frame arrived for dynamically-routed nested API key"));
        }
        else {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_API_KEY, apiKey)
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
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue(LOG_KEY_API_KEY, apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue(LOG_KEY_CLIENT_CORRELATION_ID, correlationId)
                .addKeyValue("routingMode", "dynamic")
                .log("Dispatching request to router");

        long sequence = -1;
        if (requestSource instanceof VirtualClusterRequestSource) {
            if (responseSequencer == null) {
                responseSequencer = new ResponseSequencer(ctx.channel());
            }
            sequence = responseSequencer.allocateSequence();
        }

        Integer effectiveNodeId = nodeId;
        if (requestSource instanceof RouterRequestSource && frame.targetVirtualNodeId() != Frame.NO_TARGET_VIRTUAL_NODE_ID) {
            effectiveNodeId = frame.targetVirtualNodeId();
        }
        var routingContext = new RouterContextImpl(frame, dispatcher, sessionId, subject, effectiveNodeId);

        if (frame instanceof InternalRequestFrame<?> oobFrame) {
            if (requestSource instanceof VirtualClusterRequestSource(ClientConnectionStateMachine ccsm)) {
                Objects.requireNonNull(responseSequencer).skip(sequence);
                invokeRouter(apiKey, apiVersion, frame.header(), frame.body(), routingContext,
                        (result, error) -> handleOobCompletion(ctx, oobFrame, result, error, apiKey, apiVersion, correlationId, ccsm));
            }
            else {
                invokeRouter(apiKey, apiVersion, frame.header(), frame.body(), routingContext,
                        (result, error) -> handleNestedOobCompletion(ctx, oobFrame, result, error, apiKey, apiVersion, correlationId));
            }
        }
        else {
            long seq = sequence;
            invokeRouter(apiKey, apiVersion, frame.header(), frame.body(), routingContext,
                    (result, error) -> handleCompletion(ctx, frame, result, error, apiKey, apiVersion, correlationId, seq));
        }
    }

    /**
     * Invokes {@link Router#onRequest}, treating a synchronous exception (or a {@code null}
     * returned stage) the same as an asynchronously-failed {@link CompletionStage} — per
     * {@link io.kroxylicious.proxy.router.RouterContext}'s documented contract, the runtime must
     * close the connection rather than let the exception escape to the pipeline's generic error
     * handler, which only logs and leaves the connection (and any pending OOB promise) hanging.
     */
    private void invokeRouter(ApiKeys apiKey,
                              short apiVersion,
                              RequestHeaderData header,
                              ApiMessage body,
                              RouterContextImpl routingContext,
                              BiConsumer<RouterResponse, Throwable> completion) {
        CompletionStage<RouterResponse> stage;
        try {
            stage = Objects.requireNonNull(router).onRequest(apiKey, apiVersion, header, body, routingContext);
        }
        catch (Exception t) {
            completion.accept(null, t);
            return;
        }
        if (stage == null) {
            completion.accept(null, new NullPointerException("Router.onRequest(...) returned a null CompletionStage"));
            return;
        }
        stage.whenComplete(completion);
    }

    // --- OOB completion (top-level only) ---

    @SuppressWarnings("java:S107") // difficult to reduce the number of parameters for this method
    private void handleOobCompletion(ChannelHandlerContext ctx,
                                     InternalRequestFrame<?> oobFrame,
                                     @Nullable RouterResponse result,
                                     @Nullable Throwable error,
                                     ApiKeys apiKey,
                                     short apiVersion,
                                     int correlationId,
                                     ClientConnectionStateMachine ccsm) {
        try {
            if (error != null) {
                logErrorFailedFuture(error, apiKey, correlationId);
                oobFrame.promise().completeExceptionally(error);
                ctx.channel().close().addListener(logFailure(LOGGER, "close after router returned failed future for OOB request"));
                return;
            }
            if (!(result instanceof RouterResponseImpl rri)) {
                var cause = new IllegalStateException(
                        "Router returned unrecognised RouterResponse type (apiKey=" + apiKey + ", type=" + (result == null ? "null" : result.getClass().getName()) + ")");
                LOGGER.atError()
                        .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                        .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                        .addKeyValue(LOG_KEY_API_KEY, apiKey)
                        .addKeyValue(KOG_KEY_RESULT_TYPE, result == null ? "null" : result.getClass().getName())
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
                        ? new ApiException(rwe.message())
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

    private void logErrorFailedFuture(Throwable error, ApiKeys apiKey, int correlationId) {
        LOGGER.atError()
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                .addKeyValue(LOG_KEY_API_KEY, apiKey)
                .addKeyValue(LOG_KEY_CLIENT_CORRELATION_ID, correlationId)
                .setCause(error)
                .log("Router returned failed future");
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

    // --- OOB completion (nested) ---

    // FutureReturnValueIgnored: ctx.voidPromise() is a VoidChannelPromise; by Netty's design,
    // failures on a void-promise write are delivered to the pipeline's exceptionCaught rather
    // than to a listener. Void promises are used deliberately on this hot data path to avoid
    // per-write promise allocation.
    @SuppressWarnings("FutureReturnValueIgnored")
    private void handleNestedOobCompletion(ChannelHandlerContext ctx,
                                           InternalRequestFrame<?> oobFrame,
                                           @Nullable RouterResponse result,
                                           @Nullable Throwable error,
                                           ApiKeys apiKey,
                                           short apiVersion,
                                           int correlationId) {
        if (error != null) {
            logErrorFailedFuture(error, apiKey, correlationId);
            oobFrame.promise().completeExceptionally(error);
            return;
        }
        if (!(result instanceof RouterResponseImpl rri)) {
            var cause = new IllegalStateException(
                    "Router returned unrecognised RouterResponse type (apiKey=" + apiKey + ", type=" + (result == null ? "null" : result.getClass().getName()) + ")");
            LOGGER.atError()
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_API_KEY, apiKey)
                    .addKeyValue(KOG_KEY_RESULT_TYPE, result == null ? "null" : result.getClass().getName())
                    .log("Router returned unrecognised RouterResponse type");
            oobFrame.promise().completeExceptionally(cause);
            return;
        }
        if (rri instanceof RouterResponseImpl.RespondWith rw) {
            var header = rw.header() != null ? rw.header() : new ResponseHeaderData();
            header.setCorrelationId(correlationId);
            var internalResponse = new InternalResponseFrame<>(
                    oobFrame.recipient(), apiVersion, correlationId, header, rw.body(), oobFrame.promise());
            internalResponse.setRouteName(((RouterRequestSource) requestSource).activationRoute());
            ctx.write(internalResponse, ctx.voidPromise());
            ctx.flush();
        }
        else {
            Throwable cause = rri instanceof RouterResponseImpl.RespondWithError rwe
                    ? new ApiException(rwe.message())
                    : new IllegalStateException("Router returned no-reply response for OOB request (apiKey=" + apiKey + ")");
            oobFrame.promise().completeExceptionally(cause);
            // Nested handlers ignore andCloseConnection() — they do not own the client connection
        }
    }

    // --- Regular completion (both levels) ---

    @SuppressWarnings("java:S107") // difficult to reduce the number of parameters for this method
    private void handleCompletion(ChannelHandlerContext ctx,
                                  DecodedRequestFrame<?> requestFrame,
                                  @Nullable RouterResponse result,
                                  @Nullable Throwable error,
                                  ApiKeys apiKey,
                                  short apiVersion,
                                  int correlationId,
                                  long sequence) {
        if (error != null) {
            logErrorFailedFuture(error, apiKey, correlationId);
            if (requestSource instanceof VirtualClusterRequestSource) {
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
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_API_KEY, apiKey)
                    .addKeyValue(KOG_KEY_RESULT_TYPE, result == null ? "null" : result.getClass().getName())
                    .log("Router returned unrecognised RouterResponse type; closing connection");
            if (requestSource instanceof VirtualClusterRequestSource) {
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

        deliverResponse(ctx, rri, apiKey, apiVersion, correlationId, sequence);

        if (rri.closeConnection()) {
            if (requestSource instanceof VirtualClusterRequestSource(ClientConnectionStateMachine ccsm)) {
                ccsm.requestClose(CloseReason.routerRequested());
            }
            else {
                // TODO (#4157): design proposal 070 specifies completing the future exceptionally
                // when a nested router calls andCloseConnection()
                LOGGER.atWarn()
                        .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                        .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                        .log("Nested router attempted to close connection; ignoring close request");
            }
        }

        notifyRequestComplete();
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
                ApiMessage body = rw.body();
                deliverResponseFrame(ctx, apiVersion, correlationId, header, body, sequence);
            }
            case RouterResponseImpl.RespondWithError rwe -> {
                ApiMessage message = rwe.request();
                ApiMessage errorResponse = KafkaProxyExceptionMapper.errorResponseData(apiKey, message, rwe.requestHeader().requestApiVersion(), rwe.error(),
                        rwe.message());
                if (errorResponse == null) {
                    // e.g. a Produce request with acks=0: the client isn't waiting for any response, error or not.
                    LOGGER.atTrace()
                            .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                            .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                            .addKeyValue(LOG_KEY_API_KEY, apiKey)
                            .addKeyValue(LOG_KEY_CLIENT_CORRELATION_ID, correlationId)
                            .log("Router completed request with an error that this API key does not send a response for");
                    if (responseSequencer != null) {
                        responseSequencer.skip(sequence);
                    }
                }
                else {
                    ResponseHeaderData header = new ResponseHeaderData();
                    header.setCorrelationId(correlationId);
                    deliverResponseFrame(ctx, apiVersion, correlationId, header, errorResponse, sequence);
                }
            }
            case RouterResponseImpl.RespondWithoutReply ignored -> {
                LOGGER.atTrace()
                        .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                        .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                        .addKeyValue(LOG_KEY_API_KEY, apiKey)
                        .addKeyValue(LOG_KEY_CLIENT_CORRELATION_ID, correlationId)
                        .log("Router completed request with no reply");
                if (responseSequencer != null) {
                    responseSequencer.skip(sequence);
                }
            }
        }
    }

    // FutureReturnValueIgnored: ctx.voidPromise() is a VoidChannelPromise; by Netty's design,
    // failures on a void-promise write are delivered to the pipeline's exceptionCaught rather
    // than to a listener. Void promises are used deliberately on this hot data path to avoid
    // per-write promise allocation.
    @SuppressWarnings("FutureReturnValueIgnored")
    private void deliverResponseFrame(ChannelHandlerContext ctx,
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
            if (requestSource instanceof RouterRequestSource rs) {
                responseFrame.setRouteName(rs.activationRoute());
            }
            ctx.write(responseFrame, ctx.voidPromise());
            ctx.flush();
        }
    }

    // FutureReturnValueIgnored: ctx.voidPromise() is a VoidChannelPromise; by Netty's design,
    // failures on a void-promise write are delivered to the pipeline's exceptionCaught rather
    // than to a listener. Void promises are used deliberately on this hot data path to avoid
    // per-write promise allocation.
    @SuppressWarnings("FutureReturnValueIgnored")
    private void writeErrorResponseUpstream(ChannelHandlerContext ctx,
                                            DecodedRequestFrame<?> requestFrame,
                                            Throwable error) {
        RequestHeaderData requestHeaders = requestFrame.header();
        ApiMessage message = requestFrame.body();
        String errorMessage = error.getMessage();
        ApiKeys apiKey = ApiKeys.forId(message.apiKey());
        ApiMessage body = KafkaProxyExceptionMapper.errorResponseData(apiKey, message, requestHeaders.requestApiVersion(), Errors.UNKNOWN_SERVER_ERROR, errorMessage);
        if (body == null) {
            // e.g. a Produce request with acks=0: the client isn't waiting for any response, error or not.
            LOGGER.atTrace()
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                    .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                    .addKeyValue(LOG_KEY_CLIENT_CORRELATION_ID, requestFrame.correlationId())
                    .log("Not writing error response upstream for an API key that does not send a response");
            return;
        }
        var header = new ResponseHeaderData();
        header.setCorrelationId(requestFrame.correlationId());
        var responseFrame = new DecodedResponseFrame<>(requestFrame.apiVersion(), requestFrame.correlationId(), header, body);
        if (requestSource instanceof RouterRequestSource rs) {
            responseFrame.setRouteName(rs.activationRoute());
        }
        ctx.write(responseFrame, ctx.voidPromise());
        ctx.flush();
    }

    private void notifyRequestComplete() {
        if (requestSource instanceof VirtualClusterRequestSource(ClientConnectionStateMachine ccsm)) {
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
            if (outcome == RouteDispatcher.ResponseOutcome.UNHANDLED && requestSource instanceof VirtualClusterRequestSource
                    && dispatcher.correlationIdAllocator().inRange(frame.correlationId())) {
                LOGGER.atWarn()
                        .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, virtualClusterName)
                        .addKeyValue(LOG_KEY_SESSION_ID, sessionId)
                        .addKeyValue("routingCorrelationId", frame.correlationId())
                        .log("Received response with no pending routing future");
                frame.release();
                ctx.channel().close().addListener(logFailure(LOGGER, "close after response with no pending routing future"));
                promise.setSuccess();
                return;
            }
            // Restore the outer route name so upstream route filters see the response.
            if (requestSource instanceof RouterRequestSource rs) {
                frame.setRouteName(rs.activationRoute());
            }
        }
        ctx.write(msg, promise);
    }

    // --- Router lifecycle ---

    private void ensureRouterCreated() {
        if (router == null) {
            // Only nested handlers reach this branch; top-level handlers always have a router from construction.
            var rs = (RouterRequestSource) requestSource;
            router = rs.routerChainFactory().createRouter(rs.routerName(), virtualClusterName);
        }
        if (resolvedStaticRoutes == null) {
            resolvedStaticRoutes = router.staticRoutes();
        }
    }
}
