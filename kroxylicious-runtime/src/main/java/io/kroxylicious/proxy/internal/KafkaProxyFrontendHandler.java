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
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NettySettings;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.ResponseFrame;
import io.kroxylicious.proxy.internal.ProxyChannelState.ClientActive;
import io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.codec.DecodePredicate;
import io.kroxylicious.proxy.internal.codec.KafkaMessageListener;
import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.kroxylicious.proxy.internal.filter.ApiVersionsDowngradeFilter;
import io.kroxylicious.proxy.internal.filter.ApiVersionsIntersectFilter;
import io.kroxylicious.proxy.internal.filter.BrokerAddressFilter;
import io.kroxylicious.proxy.internal.filter.EagerMetadataLearner;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.metrics.MetricEmittingKafkaMessageListener;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.ProxyChannelState.Connecting;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Forwarding;
import static io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;

public class KafkaProxyFrontendHandler
        extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyFrontendHandler.class);

    public static final int DEFAULT_IDLE_TIME_SECONDS = 31;
    public static final long DEFAULT_IDLE_SECONDS = 31L;
    private static final Long NO_TIMEOUT = null;
    private static final String AUTH_IDLE_HANDLER_NAME = "authenticatedSessionIdleHandler";

    private final EndpointReconciler endpointReconciler;
    private final DelegatingDecodePredicate dp;
    private final ProxyChannelStateMachine proxyChannelStateMachine;
    private final PluginFactoryRegistry pfr;
    private final FilterChainFactory filterChainFactory;
    private final List<NamedFilterDefinition> namedFilterDefinitions;
    private final ApiVersionsIntersectFilter apiVersionsIntersectFilter;
    private final ApiVersionsDowngradeFilter apiVersionsDowngradeFilter;

    private final Long authenticatedIdleTimeMillis;

    private @Nullable ChannelHandlerContext clientCtx;
    @VisibleForTesting
    @Nullable
    List<Object> bufferedMsgs;
    private boolean pendingClientFlushes;
    private @Nullable String sniHostname;

    // Flag if we receive a channelReadComplete() prior to outbound connection activation
    // so we can perform the channelReadComplete()/outbound flush & auto_read
    // once the outbound channel is active
    private boolean pendingReadComplete = true;

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
                              FilterChainFactory filterChainFactory,
                              List<NamedFilterDefinition> namedFilterDefinitions,
                              EndpointReconciler endpointReconciler,
                              ApiVersionsServiceImpl apiVersionsService,
                              DelegatingDecodePredicate dp,
                              ProxyChannelStateMachine proxyChannelStateMachine,
                              Optional<NettySettings> proxyNettySettings) {
        this.pfr = pfr;
        this.filterChainFactory = filterChainFactory;
        this.namedFilterDefinitions = namedFilterDefinitions;
        this.endpointReconciler = endpointReconciler;
        this.apiVersionsIntersectFilter = new ApiVersionsIntersectFilter(apiVersionsService);
        this.apiVersionsDowngradeFilter = new ApiVersionsDowngradeFilter(apiVersionsService);
        this.dp = dp;
        this.proxyChannelStateMachine = proxyChannelStateMachine;
        authenticatedIdleTimeMillis = getAuthenticatedIdleMillis(proxyNettySettings);
    }

    @Override
    public String toString() {
        // Don't include proxyChannelStateMachine's toString here
        // because proxyChannelStateMachine's toString will include the frontend's toString
        // and we don't want a SOE.
        return "KafkaProxyFrontendHandler{"
                + ", clientCtx=" + clientCtx
                + ", proxyChannelState=" + this.proxyChannelStateMachine.currentState()
                + ", number of bufferedMsgs=" + (bufferedMsgs == null ? 0 : bufferedMsgs.size())
                + ", pendingClientFlushes=" + pendingClientFlushes
                + ", sniHostname='" + sniHostname + '\''
                + ", pendingReadComplete=" + pendingReadComplete
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
            this.proxyChannelStateMachine.onClientTlsHandshakeSuccess(sslSession());
        }
        else if (event instanceof IdleStateEvent idleStateEvent && idleStateEvent.state() == IdleState.ALL_IDLE) {
            // No traffic has been observed on the channel for the configured period
            proxyChannelStateMachine.onClientIdle();
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
        this.proxyChannelStateMachine.onClientActive(this);

    }

    /**
     * Netty callback to notify that the downstream/client channel TCP connection has disconnected.
     * @param ctx The context for the downstream/client channel.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOGGER.trace("INACTIVE on inbound {}", ctx.channel());
        proxyChannelStateMachine.onClientInactive();
    }

    /**
     * Propagates backpressure to the <em>upstream/server</em> connection by notifying the {@link ProxyChannelStateMachine} when the <em>downstream/client</em> connection
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
            this.proxyChannelStateMachine.onClientWritable();
        }
        else {
            this.proxyChannelStateMachine.onClientUnwritable();
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
        proxyChannelStateMachine.onClientRequest(msg);
    }

    /**
     * Callback from the {@link ProxyChannelStateMachine} triggered when it wants to apply backpressure to the <em>downstream/client</em> connection
     */
    void applyBackpressure() {
        if (clientCtx != null) {
            this.clientCtx.channel().config().setAutoRead(false);
        }
    }

    /**
     * Callback from the {@link ProxyChannelStateMachine} triggered when it wants to remove backpressure from the <em>downstream/client</em> connection
     */
    void relieveBackpressure() {
        if (clientCtx != null) {
            this.clientCtx.channel().config().setAutoRead(true);
        }
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link ClientActive} state.
     */
    void inClientActive() {
        Channel clientChannel = clientCtx().channel();
        LOGGER.trace("{}: channelActive", clientChannel.id());
        // install filters before first read
        List<FilterAndInvoker> filters = buildFilters();
        addFiltersToPipeline(filters, clientCtx().pipeline(), clientCtx().channel());
        // Set the decode predicate now that we have the filters
        dp.setDelegate(DecodePredicate.forFilters(filters));
        // Initially the channel is not auto reading
        clientChannel.config().setAutoRead(false);
        clientChannel.read();
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link SelectingServer} state.
     */
    void inSelectingServer() {
        var target = Objects.requireNonNull(proxyChannelStateMachine.endpointBinding().upstreamTarget());
        initiateConnect(target);
    }

    @NonNull
    private List<FilterAndInvoker> buildFilters() {
        List<FilterAndInvoker> apiVersionFilters = FilterAndInvoker.build("ApiVersionsIntersect (internal)", apiVersionsIntersectFilter);
        var filterAndInvokers = new ArrayList<>(apiVersionFilters);
        filterAndInvokers.addAll(FilterAndInvoker.build("ApiVersionsDowngrade (internal)", apiVersionsDowngradeFilter));

        NettyFilterContext filterContext = new NettyFilterContext(clientCtx().channel().eventLoop(), pfr);
        List<FilterAndInvoker> filterChain = filterChainFactory.createFilters(filterContext, this.namedFilterDefinitions);
        filterAndInvokers.addAll(filterChain);

        if (proxyChannelStateMachine.endpointBinding().restrictUpstreamToMetadataDiscovery()) {
            filterAndInvokers.addAll(FilterAndInvoker.build("EagerMetadataLearner (internal)", new EagerMetadataLearner()));
        }
        filterAndInvokers.addAll(FilterAndInvoker.build("VirtualCluster TopicNameCache (internal)", proxyChannelStateMachine.virtualCluster().getTopicNameCacheFilter()));
        List<FilterAndInvoker> brokerAddressFilters = FilterAndInvoker.build("BrokerAddress (internal)",
                new BrokerAddressFilter(proxyChannelStateMachine.endpointGateway(), endpointReconciler));
        filterAndInvokers.addAll(brokerAddressFilters);

        return filterAndInvokers;
    }

    /**
     * <p>Invoked when the last message read by the current read operation
     * has been consumed by {@link #channelRead(ChannelHandlerContext, Object)}.</p>
     * This allows the proxy to batch requests.
     * @param clientCtx The client context
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext clientCtx) {
        proxyChannelStateMachine.clientReadComplete();
    }

    /**
     * Handles an exception in downstream/client pipeline by notifying {@link #proxyChannelStateMachine} of the issue.
     * @param ctx The downstream context
     * @param cause The downstream exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        proxyChannelStateMachine.onClientException(cause);
    }

    /**
     * Initiates the connection to a server.
     * Changes {@link #proxyChannelStateMachine} from {@link SelectingServer} to {@link Connecting}
     * Initializes the {@code backendHandler} and configures its pipeline
     * with the given {@code filters}.
     * @param remote upstream broker target
     */
    void initiateConnect(
                         HostPort remote) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Connecting to backend broker {}",
                    this.proxyChannelStateMachine.sessionId(), remote);
        }
        this.proxyChannelStateMachine.onInitiateConnect(remote);
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link Connecting} state.
     */
    void inConnecting(
                      HostPort remote,
                      KafkaProxyBackendHandler backendHandler) {
        final Channel inboundChannel = clientCtx().channel();
        // Start the upstream connection attempt.
        final Bootstrap bootstrap = configureBootstrap(backendHandler, inboundChannel);

        LOGGER.atDebug().setMessage("{}: Connecting to outbound {}")
                .addArgument(this.proxyChannelStateMachine::sessionId)
                .addArgument(remote)
                .log();
        ChannelFuture serverTcpConnectFuture = initConnection(remote.host(), remote.port(), bootstrap);
        Channel outboundChannel = serverTcpConnectFuture.channel();
        ChannelPipeline pipeline = outboundChannel.pipeline();

        var correlationManager = new CorrelationManager();

        // Note: Because we are acting as a client of the target cluster and are thus writing Request data to an outbound channel, the Request flows from the
        // last outbound handler in the pipeline to the first. When Responses are read from the cluster, the inbound handlers of the pipeline are invoked in
        // the reverse order, from first to last. This is the opposite of how we configure a server pipeline like we do in KafkaProxyInitializer where the channel
        // reads Kafka requests, as the message flows are reversed. This is also the opposite of the order that Filters are declared in the Kroxylicious configuration
        // file. The Netty Channel pipeline documentation provides an illustration https://netty.io/4.0/api/io/netty/channel/ChannelPipeline.html
        if (proxyChannelStateMachine.virtualCluster().isLogFrames()) {
            pipeline.addFirst("frameLogger", new LoggingHandler("io.kroxylicious.proxy.internal.UpstreamFrameLogger", LogLevel.INFO));
        }

        var encoderListener = buildMetricsMessageListenerForEncode();
        var decoderListener = buildMetricsMessageListenerForDecode();

        pipeline.addFirst("responseDecoder",
                new KafkaResponseDecoder(correlationManager, proxyChannelStateMachine.virtualCluster().socketFrameMaxSizeBytes(), decoderListener));
        pipeline.addFirst("requestEncoder", new KafkaRequestEncoder(correlationManager, encoderListener));
        if (proxyChannelStateMachine.virtualCluster().isLogNetwork()) {
            pipeline.addFirst("networkLogger", new LoggingHandler("io.kroxylicious.proxy.internal.UpstreamNetworkLogger", LogLevel.INFO));
        }
        proxyChannelStateMachine.virtualCluster().getUpstreamSslContext().ifPresent(sslContext -> {
            final SslHandler handler = sslContext.newHandler(outboundChannel.alloc(), remote.host(), remote.port());
            pipeline.addFirst("ssl", handler);
        });

        LOGGER.debug("Configured broker channel pipeline: {}", pipeline);

        serverTcpConnectFuture.addListener(future -> {
            if (future.isSuccess()) {
                LOGGER.trace("{}: Outbound connected", clientCtx().channel().id());
                // This branch does not cause the transition to Connected:
                // That happens when the backend filter call #onUpstreamChannelActive(ChannelHandlerContext).
            }
            else {
                proxyChannelStateMachine.onServerException(future.cause());
            }
        });
    }

    private MetricEmittingKafkaMessageListener buildMetricsMessageListenerForEncode() {
        var clusterName = this.proxyChannelStateMachine.clusterName();
        var nodeId = proxyChannelStateMachine.nodeId();
        var proxyToServerMessageCounterProvider = Metrics.proxyToServerMessageCounterProvider(clusterName, nodeId);
        var proxyToServerMessageSizeDistributionProvider = Metrics.proxyToServerMessageSizeDistributionProvider(clusterName,
                nodeId);
        return new MetricEmittingKafkaMessageListener(proxyToServerMessageCounterProvider, proxyToServerMessageSizeDistributionProvider);
    }

    private KafkaMessageListener buildMetricsMessageListenerForDecode() {
        var clusterName = proxyChannelStateMachine.clusterName();
        var nodeId = proxyChannelStateMachine.nodeId();
        var serverToProxyMessageCounterProvider = Metrics.serverToProxyMessageCounterProvider(clusterName, nodeId);

        var serverToProxyMessageSizeDistributionProvider = Metrics.serverToProxyMessageSizeDistributionProvider(clusterName,
                nodeId);
        return new MetricEmittingKafkaMessageListener(serverToProxyMessageCounterProvider, serverToProxyMessageSizeDistributionProvider);
    }

    /** Ugly hack used for testing */
    @VisibleForTesting
    Bootstrap configureBootstrap(KafkaProxyBackendHandler backendHandler, Channel inboundChannel) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(inboundChannel.eventLoop())
                .channel(inboundChannel.getClass())
                .handler(backendHandler)
                .option(ChannelOption.AUTO_READ, true)
                .option(ChannelOption.TCP_NODELAY, true);
        return bootstrap;
    }

    /** Ugly hack used for testing */
    @VisibleForTesting
    ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap bootstrap) {
        return bootstrap.connect(remoteHost, remotePort);
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link Forwarding} state.
     */
    void inForwarding() {
        // connection is complete, so first forward the buffered message
        if (bufferedMsgs != null) {
            for (Object bufferedMsg : bufferedMsgs) {
                proxyChannelStateMachine.messageFromClient(bufferedMsg);
            }
            bufferedMsgs = null;
        }

        if (pendingReadComplete) {
            pendingReadComplete = false;
            channelReadComplete(this.clientCtx());
        }

    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link Closed} state.
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
     * Called by the {@link ProxyChannelStateMachine} to propagate an RPC to the downstream client.
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
     * Called by the {@link ProxyChannelStateMachine} when the bach from the upstream/server side is complete.
     */
    void flushToClient() {
        final Channel inboundChannel = clientCtx().channel();
        if (pendingClientFlushes) {
            pendingClientFlushes = false;
            inboundChannel.flush();
        }
        if (!inboundChannel.isWritable()) {
            // TODO does duplicate the writeability change notification from netty? If it does is that a problem?
            proxyChannelStateMachine.onClientUnwritable();
        }
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} when there is a requirement to buffer RPC's prior to forwarding to the upstream/server.
     * Generally this is expected to be when client requests are received before we have a connection to the upstream node.
     * @param msg the RPC to buffer.
     */
    void bufferMsg(Object msg) {
        if (bufferedMsgs == null) {
            bufferedMsgs = new ArrayList<>();
        }
        bufferedMsgs.add(msg);
    }

    private void addFiltersToPipeline(
                                      List<FilterAndInvoker> filters,
                                      ChannelPipeline pipeline,
                                      Channel inboundChannel) {

        int num = 0;

        for (var protocolFilter : filters) {
            // TODO configurable timeout
            // Handler name must be unique, but filters are allowed to appear multiple times
            String handlerName = "filter-" + ++num + "-" + protocolFilter.filterName();
            pipeline.addBefore(clientCtx().name(),
                    handlerName,
                    new FilterHandler(
                            protocolFilter,
                            20000,
                            sniHostname,
                            inboundChannel,
                            proxyChannelStateMachine));
        }
    }

    void unblockClient() {
        var inboundChannel = clientCtx().channel();
        inboundChannel.config().setAutoRead(true);
        proxyChannelStateMachine.onClientWritable();
    }

    private ChannelHandlerContext clientCtx() {
        return Objects.requireNonNull(this.clientCtx, "clientCtx was null while in state " + this.proxyChannelStateMachine.currentState());
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
        ResponseFrame errorResponse;
        final Object triggerMsg = bufferedMsgs != null && !bufferedMsgs.isEmpty() ? bufferedMsgs.get(0) : null;
        if (errorCodeEx != null && triggerMsg instanceof final DecodedRequestFrame<?> triggerFrame) {
            errorResponse = buildErrorResponseFrame(triggerFrame, errorCodeEx);
        }
        else {
            errorResponse = null;
        }
        return errorResponse;
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

}
