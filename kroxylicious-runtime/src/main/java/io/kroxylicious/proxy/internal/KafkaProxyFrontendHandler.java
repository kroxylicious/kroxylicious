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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
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
import io.netty.handler.codec.DecoderException;
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
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.filter.impl.ApiVersionsDowngradeFilter;
import io.kroxylicious.proxy.internal.filter.impl.ApiVersionsIntersectFilter;
import io.kroxylicious.proxy.internal.filter.impl.BrokerAddressFilter;
import io.kroxylicious.proxy.internal.filter.impl.EagerMetadataLearner;
import io.kroxylicious.proxy.internal.net.BrokerEndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.routing.DirectRouting;
import io.kroxylicious.proxy.internal.routing.DynamicRouting;
import io.kroxylicious.proxy.internal.routing.NodeIdMapping;
import io.kroxylicious.proxy.internal.routing.RouteDescriptor;
import io.kroxylicious.proxy.internal.routing.RoutingHandler;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Netty handler for the downstream (client-facing) side of the proxy. Feeds channel lifecycle
 * and read events into the {@link ClientConnectionStateMachine}, buffers client requests until
 * the session is ready to forward them, and writes responses back to the client.
 */
@SuppressWarnings("java:S1192") // ignore dupe string literals is due to logger keys
public class KafkaProxyFrontendHandler
        extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyFrontendHandler.class);

    /** Default idle timeout, in seconds, applied to a client connection before it has authenticated. */
    public static final int DEFAULT_IDLE_TIME_SECONDS = 31;
    /** Default idle timeout, in seconds, as a {@code long} value. */
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

    /**
     * Returns the SSL session, or null if a session does not currently exist.
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
        // install filters before first read
        List<FilterAndInvoker> filters = buildFilters();
        addFiltersToPipeline(filters, clientCtx().pipeline(), clientChannel);

        var allFilters = new ArrayList<>(filters);
        allFilters.addAll(installRouteFilters(clientCtx().pipeline(), clientChannel));

        // Set the decode predicate now that we have all filters (VC + per-route)
        dp.setDelegate(DecodePredicate.forFilters(allFilters));
        // Initially the channel is not auto reading
        clientChannel.config().setAutoRead(false);
        clientChannel.read();
    }

    private List<FilterAndInvoker> installRouteFilters(ChannelPipeline pipeline, Channel clientChannel) {
        var vc = clientConnectionStateMachine.virtualCluster();
        if (!(vc.routing() instanceof DynamicRouting dr)) {
            return List.of();
        }
        var filterContext = new NettyFilterContext(clientChannel.eventLoop(), pfr);
        var allRouteFilters = new ArrayList<FilterAndInvoker>();

        // Install route filters for top-level routes first, inserting nested RoutingHandlers
        // after each router-targeting route's filters, then install nested route filters.
        for (var entry : dr.topLevelRouteDescriptors().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
            String routeName = entry.getKey();
            RouteDescriptor rd = entry.getValue();
            allRouteFilters.addAll(installFiltersForRoute(pipeline, clientChannel, filterContext, vc, routeName));
            if (rd.targetsRouter()) {
                installNestedRoutingHandler(pipeline, dr, rd.routerName(), routeName);
            }
        }
        // Install filters for nested routes (qualified names), inserting nested RoutingHandlers
        // after each router-targeting nested route's filters to support arbitrary nesting depth.
        for (var entry : dr.allRouteDescriptors().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
            String qualifiedName = entry.getKey();
            if (qualifiedName.contains("/")) {
                RouteDescriptor nestedRd = entry.getValue();
                allRouteFilters.addAll(installFiltersForRoute(pipeline, clientChannel, filterContext, vc, qualifiedName));
                if (nestedRd.targetsRouter()) {
                    installNestedRoutingHandler(pipeline, dr, nestedRd.routerName(), qualifiedName);
                }
            }
        }
        return allRouteFilters;
    }

    private List<FilterAndInvoker> installFiltersForRoute(ChannelPipeline pipeline, Channel clientChannel,
                                                          NettyFilterContext filterContext,
                                                          VirtualClusterModel vc, String routeName) {
        List<FilterAndInvoker> routeFilters = vc.createRouteFilters(routeName, filterContext);
        for (int i = 0; i < routeFilters.size(); i++) {
            FilterAndInvoker fi = routeFilters.get(i);
            String handlerName = "routeFilter-" + routeName + "-" + i + "-" + fi.filterName();
            pipeline.addBefore("routingTerminalHandler", handlerName,
                    new RouteFilterHandler(fi, 20000, sniHostname, clientChannel,
                            clientConnectionStateMachine, routeName));
        }
        return routeFilters;
    }

    private void installNestedRoutingHandler(ChannelPipeline pipeline,
                                             DynamicRouting dr,
                                             String nestedRouterName,
                                             String activationRoute) {
        var routerChainFactory = dr.routerChainFactory();
        // Build the nested router's routes and NodeIdMapping from allRouteDescriptors
        var nestedRoutes = new LinkedHashMap<String, RouteDescriptor>();
        String prefix = nestedRouterName + "/";
        for (var entry : dr.allRouteDescriptors().entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String localName = entry.getKey().substring(prefix.length());
                if (!localName.contains("/")) {
                    nestedRoutes.put(localName, entry.getValue());
                }
            }
        }
        NodeIdMapping nestedNodeIdMapping = NodeIdMapping.build(nestedRoutes);
        var topLevel = (RoutingHandler) pipeline.get("routerDispatchHandler");
        String handlerName = "nestedRoutingHandler-" + activationRoute;
        pipeline.addBefore("routingTerminalHandler", handlerName,
                RoutingHandler.nested(
                        activationRoute,
                        nestedRouterName,
                        clientConnectionStateMachine.clusterName(),
                        routerChainFactory,
                        nestedRoutes,
                        nestedNodeIdMapping,
                        topLevel.correlationIdAllocator(),
                        topLevel.routerNodeAddresses(),
                        clientConnectionStateMachine.sessionId(),
                        clientConnectionStateMachine.authenticatedSubject(),
                        clientConnectionStateMachine.endpointBinding() instanceof BrokerEndpointBinding beb ? beb.nodeId() : null));
    }

    private List<FilterAndInvoker> buildFilters() {
        List<FilterAndInvoker> apiVersionFilters = FilterAndInvoker.build("ApiVersionsIntersect (internal)", apiVersionsIntersectFilter);
        var filterAndInvokers = new ArrayList<>(apiVersionFilters);
        filterAndInvokers.addAll(FilterAndInvoker.build("ApiVersionsDowngrade (internal)", apiVersionsDowngradeFilter));

        NettyFilterContext filterContext = new NettyFilterContext(clientCtx().channel().eventLoop(), pfr);

        List<FilterAndInvoker> filterChain = clientConnectionStateMachine.virtualCluster()
                .filterChainFactory()
                .createFilters(filterContext);
        filterAndInvokers.addAll(filterChain);

        // EagerMetadataLearner is only applicable to direct-routing VCs. MetadataDiscoveryBrokerEndpointBinding
        // returns restrictUpstreamToMetadataDiscovery()=true for any VC type with per-node ports, but its
        // upstreamTarget() throws for dynamic routing — so the routing type must be checked explicitly here.
        if (clientConnectionStateMachine.endpointBinding().restrictUpstreamToMetadataDiscovery()
                && clientConnectionStateMachine.virtualCluster().routing() instanceof DirectRouting) {
            filterAndInvokers.addAll(FilterAndInvoker.build("EagerMetadataLearner (internal)", new EagerMetadataLearner()));
        }
        filterAndInvokers
                .addAll(FilterAndInvoker.build("VirtualCluster TopicNameCache (internal)", clientConnectionStateMachine.virtualCluster().getTopicNameCacheFilter()));
        List<FilterAndInvoker> brokerAddressFilters = FilterAndInvoker.build("BrokerAddress (internal)",
                new BrokerAddressFilter(clientConnectionStateMachine.endpointGateway(), endpointReconciler));
        filterAndInvokers.addAll(brokerAddressFilters);

        return filterAndInvokers;
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
    // FutureReturnValueIgnored: clientCtx().voidPromise() is a VoidChannelPromise; by Netty's
    // design, failures on a void-promise write are delivered to the pipeline's exceptionCaught
    // rather than to a listener. Void promises are used deliberately on this hot data path to
    // avoid per-write promise allocation.
    @SuppressWarnings("FutureReturnValueIgnored")
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
        int filterIndex = 0;
        String addNextFilterAfter = clientCtx().name();
        for (FilterAndInvoker protocolFilter : filters) {
            ++filterIndex;
            String handlerName = "filter-" + filterIndex + "-" + protocolFilter.filterName();
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

    @VisibleForTesting
    static @Nullable ResponseFrame buildErrorResponseFrame(
                                                           DecodedRequestFrame<?> triggerFrame,
                                                           Throwable error) {
        RequestHeaderData requestHeaders = triggerFrame.header();
        ApiMessage message = triggerFrame.body();
        String errorMessage = error.getMessage();
        ApiKeys apiKey = ApiKeys.forId(message.apiKey());
        var responseError = Errors.UNKNOWN_SERVER_ERROR;
        if (error instanceof DecoderException && error.getCause() instanceof FrameOversizedException) {
            responseError = Errors.INVALID_REQUEST;
        }
        var responseData = KafkaProxyExceptionMapper.errorResponseData(apiKey, message, requestHeaders.requestApiVersion(), responseError, errorMessage);
        if (responseData == null) {
            // e.g. a Produce request with acks=0: the client isn't waiting for any response, error or not.
            LOGGER.atTrace()
                    .addKeyValue("clientCorrelationId", triggerFrame.correlationId())
                    .log("Not sending an error response for an API key that does not send a response");
            return null;
        }
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
            triggerMsg = bufferedMsgs.getFirst();
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

    /**
     * Notifies this handler that the session has authenticated: removes the pre-session idle
     * handler and, if configured, installs the (longer) authenticated-session idle handler.
     */
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

    /**
     * Returns the host address of the connected client.
     * @return the client's host address, or the string form of the socket address if it is not an inet address
     */
    protected String remoteHost() {
        SocketAddress socketAddress = clientCtx().channel().remoteAddress();
        if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
            return inetSocketAddress.getAddress().getHostAddress();
        }
        else {
            return String.valueOf(socketAddress);
        }
    }

    /**
     * Returns the port of the connected client.
     * @return the client's port, or {@code -1} if the socket address is not an inet address
     */
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
