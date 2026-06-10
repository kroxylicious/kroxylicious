/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.config.NettySettings;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.ResponseFrame;
import io.kroxylicious.proxy.internal.ClientConnectionState.ClientActive;
import io.kroxylicious.proxy.internal.ClientConnectionState.Closed;
import io.kroxylicious.proxy.internal.codec.DecodePredicate;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.filter.impl.ApiVersionsDowngradeFilter;
import io.kroxylicious.proxy.internal.filter.impl.ApiVersionsIntersectFilter;
import io.kroxylicious.proxy.internal.filter.impl.BrokerAddressFilter;
import io.kroxylicious.proxy.internal.filter.impl.EagerMetadataLearner;
import io.kroxylicious.proxy.internal.filter.impl.TopicIdRequestEnrichmentFilter;
import io.kroxylicious.proxy.internal.filter.impl.TopicIdResponseEnrichmentFilter;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.Nullable;

@SuppressWarnings("java:S1192") // ignore dupe string literals is due to logger keys
public class KafkaProxyFrontendHandler
        extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyFrontendHandler.class);

    public static final int DEFAULT_IDLE_TIME_SECONDS = 31;
    public static final long DEFAULT_IDLE_SECONDS = 31L;
    private static final Long NO_TIMEOUT = null;
    private static final String AUTH_IDLE_HANDLER_NAME = "authenticatedSessionIdleHandler";

    private final EndpointReconciler endpointReconciler;
    private final DelegatingDecodePredicate dp;
    private final ClientConnectionStateMachine clientConnectionStateMachine;
    private final PluginFactoryRegistry pfr;
    private final ApiVersionsIntersectFilter apiVersionsIntersectFilter;
    private final ApiVersionsDowngradeFilter apiVersionsDowngradeFilter;

    private final Long authenticatedIdleTimeMillis;

    private @Nullable ChannelHandlerContext clientCtx;
    @VisibleForTesting
    @Nullable
    List<Object> bufferedMsgs;
    @VisibleForTesting
    @Nullable
    DecodedRequestFrame<?> initialRequestForError;
    private boolean pendingClientFlushes;
    private @Nullable String sniHostname;
    private @Nullable io.kroxylicious.proxy.bootstrap.RouterChainFactory routerChainFactory;
    private @Nullable java.util.Map<Integer, io.kroxylicious.proxy.service.HostPort> sharedNodeAddresses;
    private @Nullable io.kroxylicious.proxy.internal.net.EndpointBinding routingEndpointBinding;

    /**
     * @return the SSL session, or null if a session does not (currently) exist.
     */
    @Nullable
    SSLSession sslSession() {
        // The SslHandler is added to the pipeline by the SniHandler (replacing it) after the ClientHello.
        // It is added using the fully-qualified class name.
        SslHandler sslHandler = (SslHandler) this.clientCtx().pipeline().get(SslHandler.class.getName());
        return Optional.ofNullable(sslHandler)
                .map(SslHandler::engine)
                .map(SSLEngine::getSession)
                .orElse(null);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    KafkaProxyFrontendHandler(
                              PluginFactoryRegistry pfr,
                              EndpointReconciler endpointReconciler,
                              ApiVersionsServiceImpl apiVersionsService,
                              DelegatingDecodePredicate dp,
                              TransportSubjectBuilder subjectBuilder,
                              ClientConnectionStateMachine clientConnectionStateMachine,
                              Optional<NettySettings> proxyNettySettings) {
        this.pfr = pfr;
        this.endpointReconciler = endpointReconciler;
        this.apiVersionsIntersectFilter = new ApiVersionsIntersectFilter(apiVersionsService);
        this.apiVersionsDowngradeFilter = new ApiVersionsDowngradeFilter(apiVersionsService);
        this.dp = dp;
        this.clientConnectionStateMachine = clientConnectionStateMachine;
        authenticatedIdleTimeMillis = getAuthenticatedIdleMillis(proxyNettySettings);
    }

    void setRoutingConfig(io.kroxylicious.proxy.bootstrap.RouterChainFactory routerChainFactory,
                          java.util.Map<Integer, io.kroxylicious.proxy.service.HostPort> sharedNodeAddresses,
                          io.kroxylicious.proxy.internal.net.EndpointBinding binding) {
        this.routerChainFactory = routerChainFactory;
        this.sharedNodeAddresses = sharedNodeAddresses;
        this.routingEndpointBinding = binding;
    }

    @Override
    public String toString() {
        // Don't include clientConnectionStateMachine's toString here
        // because clientConnectionStateMachine's toString will include the frontend's toString
        // and we don't want a SOE.
        return "KafkaProxyFrontendHandler{"
                + ", clientCtx=" + clientCtx
                + ", proxyChannelState=" + this.clientConnectionStateMachine.currentState()
                + ", number of bufferedMsgs=" + (bufferedMsgs == null ? 0 : bufferedMsgs.size())
                + ", pendingClientFlushes=" + pendingClientFlushes
                + ", sniHostname='" + sniHostname + '\''
                + '}';
    }

    /**
     * Netty callback. Used to notify us of custom events.
     * Events such as ServerNameIndicator (SNI) resolution completing.
     * <br>
     * This method is called for <em>every</em> custom event, so its up to us to filter out the ones we care about.
     *
     * @param ctx the channel handler context on which the event was triggered.
     * @param event the information being notified
     * @throws Exception any errors in processing.
     */
    @Override
    public void userEventTriggered(
                                   ChannelHandlerContext ctx,
                                   Object event)
            throws Exception {
        if (event instanceof SniCompletionEvent sniCompletionEvent) {
            if (sniCompletionEvent.isSuccess()) {
                this.sniHostname = sniCompletionEvent.hostname();
            }
            else {
                throw new IllegalStateException("SNI failed", sniCompletionEvent.cause());
            }
        }
        else if (event instanceof SslHandshakeCompletionEvent handshakeCompletionEvent
                && handshakeCompletionEvent.isSuccess()) {
            this.clientConnectionStateMachine.onClientTlsHandshakeSuccess(sslSession());
        }
        else if (event instanceof IdleStateEvent idleStateEvent && idleStateEvent.state() == IdleState.ALL_IDLE) {
            // No traffic has been observed on the channel for the configured period
            clientConnectionStateMachine.onClientIdle();
        }

        super.userEventTriggered(ctx, event);
    }

    /**
     * Netty callback to notify that the downstream/client channel has an active TCP connection.
     * @param ctx the handler context for downstream/client channel
     * @throws Exception if we object to the client...
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.clientCtx = ctx;
        this.clientConnectionStateMachine.onClientActive(this);

    }

    /**
     * Netty callback to notify that the downstream/client channel TCP connection has disconnected.
     * @param ctx The context for the downstream/client channel.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOGGER.atTrace()
                .addKeyValue("channelId", () -> ctx.channel().toString())
                .log("INACTIVE on inbound");
        clientConnectionStateMachine.onClientInactive();
    }

    /**
     * Propagates backpressure to the <em>upstream/server</em> connection by notifying the {@link ClientConnectionStateMachine} when the <em>downstream/client</em> connection
     * blocks or unblocks.
     * @param ctx the handler context for downstream/client channel
     * @throws Exception If something went wrong
     */
    @Override
    public void channelWritabilityChanged(
                                          final ChannelHandlerContext ctx)
            throws Exception {
        super.channelWritabilityChanged(ctx);
        if (ctx.channel().isWritable()) {
            this.clientConnectionStateMachine.onClientWritable();
        }
        else {
            this.clientConnectionStateMachine.onClientUnwritable();
        }
    }

    /**
     * Netty callback that something has been read from the downstream/client channel.
     * @param ctx The context for the downstream/client channel.
     * @param msg the message read from the channel.
     */
    @Override
    public void channelRead(
                            ChannelHandlerContext ctx,
                            Object msg) {
        clientConnectionStateMachine.onClientRequest(msg);
    }

    /**
     * Callback from the {@link ClientConnectionStateMachine} triggered when it wants to apply backpressure to the <em>downstream/client</em> connection
     */
    void applyBackpressure() {
        if (clientCtx != null) {
            this.clientCtx.channel().config().setAutoRead(false);
        }
    }

    /**
     * Callback from the {@link ClientConnectionStateMachine} triggered when it wants to remove backpressure from the <em>downstream/client</em> connection
     */
    void relieveBackpressure() {
        if (clientCtx != null) {
            this.clientCtx.channel().config().setAutoRead(true);
        }
    }

    /**
     * Called by the {@link ClientConnectionStateMachine} on entry to the {@link ClientActive} state.
     */
    void inClientActive() {
        Channel clientChannel = clientCtx().channel();
        LOGGER.atTrace()
                .addKeyValue("channelId", clientChannel.id())
                .log("ChannelActive");

        if (clientConnectionStateMachine.virtualCluster().usesRouter()) {
            inClientActiveWithRouting(clientChannel);
        }
        else {
            inClientActiveWithoutRouting(clientChannel);
        }
    }

    private void inClientActiveWithoutRouting(Channel clientChannel) {
        var topicIdCache = new java.util.HashMap<org.apache.kafka.common.Uuid, String>();
        List<FilterAndInvoker> filters = buildFilters(true, topicIdCache);
        addFiltersToPipeline(filters, clientCtx().pipeline(), clientChannel);
        dp.setDelegate(DecodePredicate.forFilters(filters));
        clientChannel.config().setAutoRead(false);
        clientChannel.read();
    }

    private void inClientActiveWithRouting(Channel clientChannel) {
        ChannelPipeline pipeline = clientCtx().pipeline();
        var allFilters = new ArrayList<FilterAndInvoker>();
        NettyFilterContext filterContext = new NettyFilterContext(clientChannel.eventLoop(), pfr);
        var vc = clientConnectionStateMachine.virtualCluster();

        // 1. Build and install internal-only filters
        var topicIdCache = new java.util.HashMap<org.apache.kafka.common.Uuid, String>();
        List<FilterAndInvoker> internalFilters = buildFilters(false, topicIdCache);
        addFiltersToPipeline(internalFilters, pipeline, clientChannel);
        allFilters.addAll(internalFilters);

        // 2. Add PassthroughRoutingHandler after last internal filter
        String lastHandler = lastFilterHandlerName(pipeline);
        pipeline.addAfter(lastHandler, "passthroughRouting",
                new io.kroxylicious.proxy.internal.routing.PassthroughRoutingHandler());

        // 3. Build VC user filters and add as route filters scoped to "default"
        List<FilterAndInvoker> vcFilters = vc.filterChainFactory().createFilters(filterContext);
        allFilters.addAll(vcFilters);

        // 4. Walk the router graph depth-first, installing RoutingDecisionHandlers
        // and route filters in topological order before the RoutingTerminalHandler
        installRouterGraph(
                vc.routerName(),
                io.kroxylicious.proxy.internal.routing.PassthroughRoutingHandler.DEFAULT_ROUTE,
                java.util.function.IntUnaryOperator.identity(),
                vcFilters,
                filterContext, pipeline, clientChannel, allFilters,
                topicIdCache);

        // 4a. Install unscoped topicId response enrichment immediately before
        // the terminal handler — every backend response passes through it.
        var responseEnrichment = new TopicIdResponseEnrichmentFilter(clientConnectionStateMachine.virtualCluster().getTopicIdResponseCache());
        List<FilterAndInvoker> responseEnrichmentFai = FilterAndInvoker.build(
                "TopicIdResponseEnrichment (internal)", responseEnrichment);
        allFilters.addAll(responseEnrichmentFai);
        for (FilterAndInvoker fi : responseEnrichmentFai) {
            pipeline.addBefore("routingTerminalHandler",
                    "topicIdResponseEnrichment",
                    new FilterHandler(fi, 20000, sniHostname, clientChannel,
                            clientConnectionStateMachine));
        }

        // 5. Set top-level NodeIdMapping on CCSM for broker-port resolution
        var topRouteDescriptors = vc.routeDescriptors();
        var topRouteIds = topRouteDescriptors.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        java.util.Map.Entry::getKey,
                        e -> e.getValue().id()));
        io.kroxylicious.proxy.internal.routing.NodeIdMapping topNodeIdMapping = topRouteIds.size() > 1
                ? new io.kroxylicious.proxy.internal.routing.BijectiveNodeIdMapping(topRouteIds, topRouteIds.size())
                : new io.kroxylicious.proxy.internal.routing.IdentityNodeIdMapping(topRouteIds.keySet().iterator().next());
        clientConnectionStateMachine.setNodeIdMapping(topNodeIdMapping);

        // 6. Set decode predicate to union of ALL filters across the routing DAG
        dp.setDelegate(DecodePredicate.forFilters(allFilters));
        clientChannel.config().setAutoRead(false);
        clientChannel.read();
    }

    private void installRouterGraph(String routerName,
                                    String activationRoute,
                                    java.util.function.IntUnaryOperator parentTranslator,
                                    List<FilterAndInvoker> pendingRouteFilters,
                                    NettyFilterContext filterContext,
                                    ChannelPipeline pipeline,
                                    Channel clientChannel,
                                    List<FilterAndInvoker> allFilters,
                                    java.util.Map<org.apache.kafka.common.Uuid, String> topicIdCache) {
        var vc = clientConnectionStateMachine.virtualCluster();
        var allRouteDescriptors = vc.allRouteDescriptors();
        var routeDescriptors = allRouteDescriptors != null ? allRouteDescriptors.get(routerName) : vc.routeDescriptors();

        // Create Router and compute NodeIdMapping
        io.kroxylicious.proxy.router.Router router = routerChainFactory.createRouter(routerName, vc.getClusterName());
        var routeIds = routeDescriptors.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        java.util.Map.Entry::getKey,
                        e -> e.getValue().id()));
        io.kroxylicious.proxy.internal.routing.NodeIdMapping nodeIdMapping = routeIds.size() > 1
                ? new io.kroxylicious.proxy.internal.routing.BijectiveNodeIdMapping(routeIds, routeIds.size())
                : new io.kroxylicious.proxy.internal.routing.IdentityNodeIdMapping(routeIds.keySet().iterator().next());

        // Compose the virtual ID translator for this level
        java.util.function.IntUnaryOperator translator = parentTranslator;

        // Metrics
        var clusterName = vc.getClusterName();
        var nodeId = routingEndpointBinding.nodeId();
        var routingRequestsCounter = io.kroxylicious.proxy.internal.util.Metrics.routingRequestsCounter(clusterName, nodeId);
        var routingErrorsCounter = io.kroxylicious.proxy.internal.util.Metrics.routingErrorsCounter(clusterName, nodeId);
        var routingRequestDurationTimer = io.kroxylicious.proxy.internal.util.Metrics.routingRequestDurationTimer(clusterName, nodeId);
        var pendingResponseCount = new java.util.concurrent.atomic.AtomicInteger();
        io.kroxylicious.proxy.internal.util.Metrics.routingPendingResponsesGauge(clusterName, nodeId, pendingResponseCount);

        // Install RoutingDecisionHandler
        String handlerName = "routingDecisionHandler-" + routerName;
        var decisionHandler = new io.kroxylicious.proxy.internal.routing.RoutingDecisionHandler(
                activationRoute,
                router, routeDescriptors, router.staticRoutes(), clientConnectionStateMachine,
                nodeIdMapping,
                routingRequestsCounter, routingErrorsCounter,
                routingRequestDurationTimer, pendingResponseCount,
                translator,
                sharedNodeAddresses,
                topicIdCache);
        pipeline.addBefore("routingTerminalHandler", handlerName, decisionHandler);

        // Install any pending route filters (e.g. VC filters for "default" activation route)
        // between the handler that set this activation route and this decision handler
        for (int i = 0; i < pendingRouteFilters.size(); i++) {
            FilterAndInvoker fi = pendingRouteFilters.get(i);
            String filterName = "routeFilter-" + activationRoute + "-" + i + "-" + fi.filterName();
            pipeline.addBefore(handlerName, filterName,
                    new RouteFilterHandler(fi, 20000, sniHostname, clientChannel,
                            clientConnectionStateMachine, activationRoute));
        }

        // For each route, install route filters and recurse into nested routers
        for (var entry : routeDescriptors.entrySet()) {
            String routeName = entry.getKey();
            io.kroxylicious.proxy.internal.routing.RouteDescriptor rd = entry.getValue();

            // Build this route's filters
            List<FilterAndInvoker> routeFilters = vc.createRouteFilters(routerName, routeName, filterContext);
            allFilters.addAll(routeFilters);

            if (rd.targetsRouter()) {
                // Nested router: compose translator and recurse
                java.util.function.IntUnaryOperator nestedTranslator = nestedVirtual -> parentTranslator.applyAsInt(
                        nodeIdMapping.toVirtual(routeName, nestedVirtual));
                installRouterGraph(rd.routerName(), routeName, nestedTranslator,
                        routeFilters, filterContext, pipeline, clientChannel, allFilters,
                        topicIdCache);
            }
            else {
                // Cluster-targeting route: install route filters before terminal
                for (FilterAndInvoker fi : routeFilters) {
                    String filterHandlerName = "routeFilter-" + routeName + "-" + fi.filterName();
                    pipeline.addBefore("routingTerminalHandler", filterHandlerName,
                            new RouteFilterHandler(fi, 20000, sniHostname, clientChannel,
                                    clientConnectionStateMachine, routeName));
                }
            }
        }
    }

    /**
     * @param includeUserFilters whether to include user-configured VC filters
     * @param topicIdCache per-connection cache shared with {@link io.kroxylicious.proxy.router.RouterContext#topicName}
     */
    private List<FilterAndInvoker> buildFilters(boolean includeUserFilters,
                                                java.util.Map<org.apache.kafka.common.Uuid, String> topicIdCache) {
        List<FilterAndInvoker> apiVersionFilters = FilterAndInvoker.build("ApiVersionsIntersect (internal)", apiVersionsIntersectFilter);
        var filterAndInvokers = new ArrayList<>(apiVersionFilters);
        filterAndInvokers.addAll(FilterAndInvoker.build("ApiVersionsDowngrade (internal)", apiVersionsDowngradeFilter));
        filterAndInvokers.addAll(FilterAndInvoker.build("TopicIdRequestEnrichment (internal)", new TopicIdRequestEnrichmentFilter(topicIdCache)));

        if (includeUserFilters) {
            NettyFilterContext filterContext = new NettyFilterContext(clientCtx().channel().eventLoop(), pfr);
            List<FilterAndInvoker> filterChain = clientConnectionStateMachine.virtualCluster().filterChainFactory().createFilters(filterContext);
            filterAndInvokers.addAll(filterChain);
        }

        if (clientConnectionStateMachine.endpointBinding().restrictUpstreamToMetadataDiscovery()) {
            boolean closeAfterLearning = !clientConnectionStateMachine.virtualCluster().usesRouter();
            filterAndInvokers.addAll(FilterAndInvoker.build("EagerMetadataLearner (internal)", new EagerMetadataLearner(closeAfterLearning)));
        }
        filterAndInvokers
                .addAll(FilterAndInvoker.build("VirtualCluster TopicNameCache (internal)", clientConnectionStateMachine.virtualCluster().getTopicNameCacheFilter()));
        List<FilterAndInvoker> brokerAddressFilters = FilterAndInvoker.build("BrokerAddress (internal)",
                new BrokerAddressFilter(clientConnectionStateMachine.endpointGateway(), endpointReconciler));
        filterAndInvokers.addAll(brokerAddressFilters);
        filterAndInvokers.addAll(FilterAndInvoker.build("TopicIdResponseEnrichment (internal)",
                new TopicIdResponseEnrichmentFilter(clientConnectionStateMachine.virtualCluster().getTopicIdResponseCache())));

        return filterAndInvokers;
    }

    private String lastFilterHandlerName(ChannelPipeline pipeline) {
        String last = clientCtx().name();
        for (var name : pipeline.names()) {
            if (name.startsWith("filter-")) {
                last = name;
            }
        }
        return last;
    }

    private void addRouteFiltersToPipeline(List<FilterAndInvoker> filters,
                                           String routeName,
                                           String addBeforeHandler,
                                           ChannelPipeline pipeline,
                                           Channel inboundChannel) {
        for (FilterAndInvoker protocolFilter : filters) {
            String handlerName = "routeFilter-" + routeName + "-" + protocolFilter.filterName();
            pipeline.addBefore(addBeforeHandler,
                    handlerName,
                    new RouteFilterHandler(
                            protocolFilter,
                            20000,
                            sniHostname,
                            inboundChannel,
                            clientConnectionStateMachine,
                            routeName));
        }
    }

    /**
     * Handles an exception in downstream/client pipeline by notifying {@link #clientConnectionStateMachine} of the issue.
     * @param ctx The downstream context
     * @param cause The downstream exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        clientConnectionStateMachine.onClientException(cause);
    }

    /**
     * Called by the {@link ClientConnectionStateMachine} on entry to the {@link Closed} state.
     */
    void inClosed(@Nullable Throwable errorCodeEx) {
        Channel inboundChannel = clientCtx().channel();
        if (inboundChannel.isActive()) {
            Object msg = null;
            if (errorCodeEx != null) {
                msg = errorResponse(errorCodeEx);
            }
            if (msg == null) {
                msg = Unpooled.EMPTY_BUFFER;
            }
            inboundChannel.writeAndFlush(msg)
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    /**
     * Called by the {@link ClientConnectionStateMachine} to propagate an RPC to the downstream client.
     * @param msg the RPC to forward.
     */
    void forwardToClient(Object msg) {
        final Channel inboundChannel = clientCtx().channel();
        if (inboundChannel.isWritable()) {
            inboundChannel.write(msg, clientCtx().voidPromise());
            pendingClientFlushes = true;
        }
        else {
            inboundChannel.writeAndFlush(msg, clientCtx().voidPromise());
            pendingClientFlushes = false;
        }
    }

    /**
     * Called by the {@link ClientConnectionStateMachine} when the bach from the upstream/server side is complete.
     */
    void flushToClient() {
        final Channel inboundChannel = clientCtx().channel();
        if (pendingClientFlushes) {
            pendingClientFlushes = false;
            inboundChannel.flush();
        }
        if (!inboundChannel.isWritable()) {
            // TODO does duplicate the writeability change notification from netty? If it does is that a problem?
            clientConnectionStateMachine.onClientUnwritable();
        }
    }

    /**
     * Called by the {@link ClientConnectionStateMachine} when there is a requirement to buffer RPC's prior to forwarding to the upstream/server.
     * Generally this is expected to be when client requests are received before we have a connection to the upstream node.
     * @param msg the RPC to buffer.
     */
    void bufferMsg(Object msg) {
        if (bufferedMsgs == null) {
            bufferedMsgs = new ArrayList<>();
            if (msg instanceof DecodedRequestFrame<?> frame) {
                initialRequestForError = frame;
            }
        }
        bufferedMsgs.add(msg);
    }

    /**
     * Add Filters after this handler in the pipeline.
     */
    private void addFiltersToPipeline(
                                      List<FilterAndInvoker> filters,
                                      ChannelPipeline pipeline,
                                      Channel inboundChannel) {
        int i = 0;
        String addNextFilterAfter = clientCtx().name();
        for (FilterAndInvoker protocolFilter : filters) {
            String handlerName = "filter-" + (++i) + "-" + protocolFilter.filterName();
            pipeline.addAfter(addNextFilterAfter,
                    handlerName,
                    new FilterHandler(
                            protocolFilter,
                            20000,
                            sniHostname,
                            inboundChannel,
                            clientConnectionStateMachine));
            addNextFilterAfter = handlerName;
        }
    }

    void unblockClient() {
        var inboundChannel = clientCtx().channel();
        forwardBufferedMessages();
        inboundChannel.config().setAutoRead(true);
        clientConnectionStateMachine.onClientWritable();
    }

    private void forwardBufferedMessages() {
        // connection is complete, so first forward the buffered message
        if (bufferedMsgs != null) {
            for (Object bufferedMsg : bufferedMsgs) {
                admitToFilterChain(bufferedMsg);
            }
            bufferedMsgs = null;
        }
    }

    Executor eventLoopExecutor() {
        return Objects.requireNonNull(clientCtx().executor(), "executor must not be null");
    }

    private ChannelHandlerContext clientCtx() {
        return Objects.requireNonNull(this.clientCtx, "clientCtx was null while in state " + this.clientConnectionStateMachine.currentState());
    }

    /**
     * Returns the downstream (client→proxy) channel, or null if not yet active.
     */
    @Nullable
    Channel clientChannel() {
        return this.clientCtx != null ? this.clientCtx.channel() : null;
    }

    private static ResponseFrame buildErrorResponseFrame(
                                                         DecodedRequestFrame<?> triggerFrame,
                                                         Throwable error) {
        var responseData = KafkaProxyExceptionMapper.errorResponseMessage(triggerFrame, error);
        final ResponseHeaderData responseHeaderData = new ResponseHeaderData();
        responseHeaderData.setCorrelationId(triggerFrame.correlationId());
        return new DecodedResponseFrame<>(triggerFrame.apiVersion(), triggerFrame.correlationId(), responseHeaderData, responseData);
    }

    /**
     * Return an error response to send to the client, or null if no response should be sent.
     * @param errorCodeEx The exception
     * @return The response frame
     */
    private @Nullable ResponseFrame errorResponse(
                                                  @Nullable Throwable errorCodeEx) {
        final Object triggerMsg;
        if (bufferedMsgs != null && !bufferedMsgs.isEmpty()) {
            triggerMsg = bufferedMsgs.get(0);
        }
        else {
            triggerMsg = initialRequestForError;
        }
        if (errorCodeEx != null && triggerMsg instanceof final DecodedRequestFrame<?> triggerFrame) {
            return buildErrorResponseFrame(triggerFrame, errorCodeEx);
        }
        return null;
    }

    /**
     * Get the ID of the frontend channel if available.
     * @return <code>null</code> if the channel is not yet available
     */
    @CheckReturnValue
    @Nullable
    public ChannelId channelId() {
        Channel channel = this.clientCtx != null ? this.clientCtx.channel() : null;
        return channel != null ? channel.id() : null;
    }

    public void onSessionAuthenticated() {
        ChannelPipeline channelPipeline = Objects.requireNonNull(clientCtx).pipeline();
        ChannelHandler preSessionHandler = channelPipeline.get(KafkaProxyInitializer.PRE_SESSION_IDLE_HANDLER);
        // sessions can be re-authenticated however we only need to act on the first instance or it may already have been removed due to MTLS auth
        if (preSessionHandler != null) {
            channelPipeline.remove(preSessionHandler);
        }
        if (!Objects.isNull(authenticatedIdleTimeMillis) && !channelPipeline.names().contains(AUTH_IDLE_HANDLER_NAME)) {
            channelPipeline.addFirst(AUTH_IDLE_HANDLER_NAME,
                    new IdleStateHandler(0, 0, authenticatedIdleTimeMillis, TimeUnit.MILLISECONDS));
        }
    }

    protected String remoteHost() {
        SocketAddress socketAddress = clientCtx().channel().remoteAddress();
        if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
            return inetSocketAddress.getAddress().getHostAddress();
        }
        else {
            return String.valueOf(socketAddress);
        }
    }

    protected int remotePort() {
        SocketAddress socketAddress = clientCtx().channel().remoteAddress();
        if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
            return inetSocketAddress.getPort();
        }
        else {
            return -1;
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nullable
    private Long getAuthenticatedIdleMillis(Optional<NettySettings> nettySettings) {
        return nettySettings.flatMap(NettySettings::authenticatedIdleTimeout).map(Duration::toMillis).orElse(NO_TIMEOUT);
    }

    /**
     * Forward a message towards the Filter Chain
     * @param msg message
     */
    public void admitToFilterChain(Object msg) {
        clientCtx().fireChannelRead(msg);
    }
}
