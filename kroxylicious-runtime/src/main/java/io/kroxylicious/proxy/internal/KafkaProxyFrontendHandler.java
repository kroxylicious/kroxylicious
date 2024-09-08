/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseDataJsonConverter;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.frame.ResponseFrame;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.codec.DecodePredicate;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaProxyFrontendHandler
        extends ChannelInboundHandlerAdapter
        implements NetFilter.NetFilterContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyFrontendHandler.class);

    /** Cache ApiVersions response which we use when returning ApiVersions ourselves */
    private static final ApiVersionsResponseData API_VERSIONS_RESPONSE;
    public static final ApiException ERROR_NEGOTIATING_SSL_CONNECTION = new NetworkException("Error negotiating SSL connection");
    public static final AttributeKey<Object> UPSTREAM_PEER_KEY = AttributeKey.valueOf("upstreamPeer");

    static {
        var objectMapper = new ObjectMapper();
        try (var parser = KafkaProxyFrontendHandler.class.getResourceAsStream("/ApiVersions-3.2.json")) {
            API_VERSIONS_RESPONSE = ApiVersionsResponseDataJsonConverter.read(objectMapper.readTree(parser), (short) 3);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final boolean logNetwork;
    private final boolean logFrames;
    private final VirtualCluster virtualCluster;

    private ChannelHandlerContext outboundCtx;
    private KafkaProxyBackendHandler backendHandler;
    private boolean pendingFlushes;

    private final NetFilter filter;
    private final SaslDecodePredicate dp;

    private AuthenticationEvent authentication;
    private String clientSoftwareName;
    private String clientSoftwareVersion;
    private String sniHostname;

    private ChannelHandlerContext inboundCtx;

    // Messages buffered while we connect to the outbound cluster
    // The size should be limited because auto read is disabled until outbound
    // channel activation
    private List<Object> bufferedMsgs = new ArrayList<>();

    // Flag if we receive a channelReadComplete() prior to outbound connection activation
    // so we can perform the channelReadComplete()/outbound flush & auto_read
    // once the outbound channel is active
    private boolean pendingReadComplete = true;

    @VisibleForTesting
    enum State {
        /** The initial state */
        START,
        /** An HAProxy message has been received */
        HA_PROXY,
        /** A Kafka ApiVersions request has been received */
        API_VERSIONS,
        /** Some other Kafka request has been received and we're in the process of connecting to the outbound cluster */
        CONNECTING,
        /** The outbound connection is connected but not yet active */
        CONNECTED,
        /** The outbound connection is active */
        OUTBOUND_ACTIVE,
        /** The outbound is connected but still TLS is still being negotiated */
        NEGOTIATING_TLS,
        /** The connection to the outbound cluster failed */
        FAILED
    }

    /**
     * The current state.
     * Transitions:
     * <code><pre>
     *    START ──→ HA_PROXY ──→ API_VERSIONS ─╭─→ CONNECTING ──→ CONNECTED ────→ OUTBOUND_ACTIVE
     *      ╰──────────╰──────────────╰────────╯        ⎸             ⎸                      ↑
     *                                                  ⎸             ⎸                      ⎸
     *                                                  ╰──→ FAILED  ╰──→ NEGOTIATING_TLS ──╯
     *                                                         ↑              ⎸
     *                                                         ╰──────────────╯
     * </pre></code>
     * Unexpected state transitions and exceptions also cause a
     * transition to {@link State#FAILED} (via {@link #illegalState(String)}}
     */
    private State state = State.START;

    private boolean isInboundBlocked = true;
    private HAProxyMessage haProxyMessage;

    KafkaProxyFrontendHandler(NetFilter filter,
                              SaslDecodePredicate dp,
                              VirtualCluster virtualCluster) {
        this.filter = filter;
        this.dp = dp;
        this.virtualCluster = virtualCluster;
        this.logNetwork = virtualCluster.isLogNetwork();
        this.logFrames = virtualCluster.isLogFrames();
    }

    private IllegalStateException illegalState(String msg) {
        String name = state.name();
        state = State.FAILED;
        return new IllegalStateException((msg == null ? "" : msg + ", ") + "state=" + name);
    }

    @VisibleForTesting
    State state() {
        return state;
    }

    /**
     * Called when the outbound channel to the upstream broker becomes active
     * @param upstreamCtx
     */
    void onUpstreamChannelActive(ChannelHandlerContext upstreamCtx) {
        LOGGER.trace("{}: outboundChannelActive: {}", inboundCtx.channel().id(), upstreamCtx.channel().id());
        this.outboundCtx = upstreamCtx;
        this.state = State.CONNECTED;
        virtualCluster.getUpstreamSslContext().ifPresentOrElse(
                this::startUpstreamTlsNegotiation,
                this::onUpstreamChannelUsable);
    }

    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        // this is key to propagate back-pressure changes
        if (backendHandler != null) {
            backendHandler.inboundChannelWritabilityChanged(ctx);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (state == State.OUTBOUND_ACTIVE) { // post-backend connection
            forwardOutbound(ctx, msg);
        }
        else {
            handlePreOutboundActive(ctx, msg);
        }
    }

    private void handlePreOutboundActive(ChannelHandlerContext ctx, Object msg) {
        if (isInitialHaProxyMessage(msg)) {
            this.haProxyMessage = (HAProxyMessage) msg;
            state = State.HA_PROXY;
        }
        else if (isInitialDecodedApiVersionsFrame(msg)) {
            handleApiVersionsFrame(ctx, msg);
        }
        else if (isInitialRequestFrame(msg)) {
            bufferMsgAndSelectServer(msg);
        }
        else if (isSubsequentRequestFrame(msg)) {
            bufferMessage(msg);
        }
        else {
            throw illegalState("Unexpected channelRead() message of " + msg.getClass());
        }
    }

    private void handleApiVersionsFrame(ChannelHandlerContext ctx, Object msg) {
        state = State.API_VERSIONS;
        DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
        storeApiVersionsFeatures(apiVersionsFrame);
        if (dp.isAuthenticationOffloadEnabled()) {
            // This handler can respond to ApiVersions itself
            writeApiVersionsResponse(ctx, apiVersionsFrame);
            // Request to read the following request
            ctx.channel().read();
        }
        else {
            bufferMsgAndSelectServer(msg);
        }
    }

    private boolean isSubsequentRequestFrame(Object msg) {
        return (state == State.CONNECTING || state == State.CONNECTED) && msg instanceof RequestFrame;
    }

    private boolean isInitialRequestFrame(Object msg) {
        return (state == State.START
                || state == State.HA_PROXY
                || state == State.API_VERSIONS)
                && msg instanceof RequestFrame;
    }

    private boolean isInitialHaProxyMessage(Object msg) {
        return state == State.START
                && msg instanceof HAProxyMessage;
    }

    private boolean isInitialDecodedApiVersionsFrame(Object msg) {
        return (state == State.START
                || state == State.HA_PROXY)
                && msg instanceof DecodedRequestFrame
                && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS;
    }

    private void bufferMsgAndSelectServer(Object msg) {
        state = State.CONNECTING;
        // But for any other request we'll need a backend connection
        // (for which we need to ask the filter which cluster to connect to
        // and with what filters)
        bufferMessage(msg);
        // TODO ensure that the filter makes exactly one upstream connection?
        // Or not for the topic routing case

        // Note filter.upstreamBroker will call back on the connect() method below
        filter.selectServer(this);
    }

    @VisibleForTesting
    void bufferMessage(Object msg) {
        this.bufferedMsgs.add(msg);
    }

    @Override
    public void initiateConnect(HostPort remote, List<FilterAndInvoker> filters) {
        if (backendHandler != null) {
            throw new IllegalStateException();
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Connecting to backend broker {} using filters {}",
                    inboundCtx.channel().id(), remote, filters);
        }
        var correlationManager = new CorrelationManager();

        final Channel inboundChannel = inboundCtx.channel();

        // Start the upstream connection attempt.
        Bootstrap b = new Bootstrap();
        backendHandler = new KafkaProxyBackendHandler(this, inboundCtx);
        b.group(inboundChannel.eventLoop())
                .channel(inboundChannel.getClass())
                .handler(backendHandler)
                .option(ChannelOption.AUTO_READ, true)
                .option(ChannelOption.TCP_NODELAY, true);

        LOGGER.trace("Connecting to outbound {}", remote);
        ChannelFuture tcpConnectFuture = initConnection(remote.host(), remote.port(), b);
        Channel outboundChannel = tcpConnectFuture.channel();
        ChannelPipeline pipeline = outboundChannel.pipeline();

        outboundChannel.attr(UPSTREAM_PEER_KEY).set(remote);

        // Note: Because we are acting as a client of the target cluster and are thus writing Request data to an outbound channel, the Request flows from the
        // last outbound handler in the pipeline to the first. When Responses are read from the cluster, the inbound handlers of the pipeline are invoked in
        // the reverse order, from first to last. This is the opposite of how we configure a server pipeline like we do in KafkaProxyInitializer where the channel
        // reads Kafka requests, as the message flows are reversed. This is also the opposite of the order that Filters are declared in the Kroxylicious configuration
        // file. The Netty Channel pipeline documentation provides an illustration https://netty.io/4.0/api/io/netty/channel/ChannelPipeline.html
        if (logFrames) {
            pipeline.addFirst("frameLogger", new LoggingHandler("io.kroxylicious.proxy.internal.UpstreamFrameLogger"));
        }
        addFiltersToPipeline(filters, pipeline, inboundChannel);
        pipeline.addFirst("responseDecoder", new KafkaResponseDecoder(correlationManager, virtualCluster.socketFrameMaxSizeBytes()));
        pipeline.addFirst("requestEncoder", new KafkaRequestEncoder(correlationManager));
        if (logNetwork) {
            pipeline.addFirst("networkLogger", new LoggingHandler("io.kroxylicious.proxy.internal.UpstreamNetworkLogger"));
        }
        virtualCluster.getUpstreamSslContext().ifPresent(sslContext -> {
            final SslHandler handler = sslContext.newHandler(outboundChannel.alloc(), remote.host(), remote.port());
            pipeline.addFirst("ssl", handler);
        });

        tcpConnectFuture.addListener(future -> {
            if (future.isSuccess()) {
                onTcpConnection(filters);
            }
            else {
                onUpstreamChannelFailed(remote, future.cause());
            }
        });
    }

    private @NonNull ResponseFrame buildErrorResponseFrame(DecodedRequestFrame<?> triggerFrame, Throwable error) {
        var responseData = KafkaProxyExceptionMapper.errorResponseMessage(triggerFrame, error);
        final ResponseHeaderData responseHeaderData = new ResponseHeaderData();
        responseHeaderData.setCorrelationId(triggerFrame.correlationId());
        return new DecodedResponseFrame<>(triggerFrame.apiVersion(), triggerFrame.correlationId(), responseHeaderData, responseData);
    }

    /**
     * Called when the channel to the upstream broker is failed.
     */
    @VisibleForTesting
    void onUpstreamChannelFailed(HostPort remote, Throwable upstreamFailure) {
        toFailedFromUpstreamException(remote, upstreamFailure);
    }

    private void toFailedFromDownstreamException(Throwable downstreamException) {
        // Close the connection if the connection attempt has failed.
        Throwable errorCodeEx = null;

        if (downstreamException instanceof DecoderException de
                && de.getCause() instanceof FrameOversizedException e) {
            var tlsHint = virtualCluster.getDownstreamSslContext().isPresent() ? "" : " or an unexpected TLS handshake";
            LOGGER.warn(
                    "Received over-sized frame, max frame size bytes {}, received frame size bytes {} "
                            + "(hint: are we decoding a Kafka frame, or something unexpected like an HTTP request{}?)",
                    e.getMaxFrameSizeBytes(), e.getReceivedFrameSizeBytes(), tlsHint);
            errorCodeEx = ERROR_NEGOTIATING_SSL_CONNECTION;
        }
        else {
            LOGGER.warn("Netty caught exception from the frontend: {}", downstreamException.getMessage(), downstreamException);
            errorCodeEx = ERROR_NEGOTIATING_SSL_CONNECTION;
        }
        toFailed(errorCodeEx);
    }

    private void toFailedFromUpstreamException(HostPort remote, Throwable upstreamFailure) {
        // Close the connection if the connection attempt has failed.
        Throwable errorCodeEx = null;
        if (upstreamFailure instanceof SSLHandshakeException e) {
            LOGGER.atInfo()
                    .setCause(LOGGER.isDebugEnabled() ? e : null)
                    .addArgument(inboundCtx.channel().id())
                    .addArgument(outboundCtx.channel().attr(UPSTREAM_PEER_KEY).get())
                    .addArgument(e.getMessage())
                    .log("{}: unable to complete TLS negotiation with {} due to: {} for further details enable debug logging");
            errorCodeEx = ERROR_NEGOTIATING_SSL_CONNECTION;
        }
        else {
            LOGGER.atWarn()
                    .setCause(LOGGER.isDebugEnabled() ? upstreamFailure : null)
                    .log("Connection to target cluster on {} failed with: {}, closing inbound channel. Increase log level to DEBUG for stacktrace",
                            remote, upstreamFailure.getMessage());
            errorCodeEx = null;
        }
        toFailed(errorCodeEx);
    }

    private void toFailed(Throwable errorCodeEx) {
        state = State.FAILED;
        ResponseFrame errorResponse;
        final Object triggerMsg = bufferedMsgs != null && !bufferedMsgs.isEmpty() ? bufferedMsgs.get(0) : null;
        if (errorCodeEx != null && triggerMsg instanceof final DecodedRequestFrame<?> triggerFrame) {
            errorResponse = buildErrorResponseFrame(triggerFrame, errorCodeEx);
        }
        else {
            errorResponse = null;
        }
        closeInboundWith(errorResponse);
    }

    private void onTcpConnection(List<FilterAndInvoker> filters) {
        LOGGER.trace("{}: Outbound connected", inboundCtx.channel().id());
        // Now we know which filters are to be used we need to update the DecodePredicate
        // so that the decoder starts decoding the messages that the filters want to intercept
        dp.setDelegate(DecodePredicate.forFilters(filters));
    }

    @VisibleForTesting
    ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap b) {
        return b.connect(remoteHost, remotePort);
    }

    private void addFiltersToPipeline(List<FilterAndInvoker> filters, ChannelPipeline pipeline, Channel inboundChannel) {
        for (var filter : filters) {
            // TODO configurable timeout
            pipeline.addFirst(filter.toString(), new FilterHandler(filter, 20000, sniHostname, virtualCluster, inboundChannel));
        }
    }

    public void forwardOutbound(final ChannelHandlerContext ctx, Object msg) {
        if (outboundCtx == null) {
            LOGGER.trace("READ on inbound {} ignored because outbound is not active (msg: {})",
                    ctx.channel(), msg);
            return;
        }
        final Channel outboundChannel = outboundCtx.channel();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("READ on inbound {} outbound {} (outbound.isWritable: {}, msg: {})",
                    ctx.channel(), outboundChannel, outboundChannel.isWritable(), msg);
            LOGGER.trace("Outbound bytesBeforeUnwritable: {}", outboundChannel.bytesBeforeUnwritable());
            LOGGER.trace("Outbound config: {}", outboundChannel.config());
            LOGGER.trace("Outbound is active, writing and flushing {}", msg);
        }
        if (outboundChannel.isWritable()) {
            outboundChannel.write(msg, outboundCtx.voidPromise());
            pendingFlushes = true;
        }
        else {
            outboundChannel.writeAndFlush(msg, outboundCtx.voidPromise());
            pendingFlushes = false;
        }
        LOGGER.trace("/READ");
    }

    /**
     * Sends an ApiVersions response from this handler to the client
     * (i.e. prior to having backend connection)
     */
    private void writeApiVersionsResponse(ChannelHandlerContext ctx, DecodedRequestFrame<ApiVersionsRequestData> frame) {

        short apiVersion = frame.apiVersion();
        int correlationId = frame.correlationId();
        ResponseHeaderData header = new ResponseHeaderData()
                .setCorrelationId(correlationId);
        LOGGER.debug("{}: Writing ApiVersions response", ctx.channel());
        ctx.writeAndFlush(new DecodedResponseFrame<>(
                apiVersion, correlationId, header, API_VERSIONS_RESPONSE));
    }

    private void storeApiVersionsFeatures(DecodedRequestFrame<ApiVersionsRequestData> frame) {
        // TODO check the format of the strings using a regex
        // Needed to reproduce the exact behaviour for how a broker handles this
        // see org.apache.kafka.common.requests.ApiVersionsRequest#isValid()
        this.clientSoftwareName = frame.body().clientSoftwareName();
        this.clientSoftwareVersion = frame.body().clientSoftwareVersion();
    }

    public void outboundWritabilityChanged(ChannelHandlerContext outboundCtx) {
        if (this.outboundCtx != outboundCtx) {
            throw illegalState("Mismatching outboundCtx");
        }
        if (isInboundBlocked && outboundCtx.channel().isWritable()) {
            isInboundBlocked = false;
            inboundCtx.channel().config().setAutoRead(true);
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
        if (outboundCtx == null) {
            LOGGER.trace("READ_COMPLETE on inbound {}, ignored because outbound is not active",
                    ctx.channel());
            pendingReadComplete = true;
            return;
        }
        final Channel outboundChannel = outboundCtx.channel();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("READ_COMPLETE on inbound {} outbound {} (pendingFlushes: {}, isInboundBlocked: {}, output.isWritable: {})",
                    ctx.channel(), outboundChannel,
                    pendingFlushes, isInboundBlocked, outboundChannel.isWritable());
        }
        if (pendingFlushes) {
            pendingFlushes = false;
            outboundChannel.flush();
        }
        if (!outboundChannel.isWritable()) {
            ctx.channel().config().setAutoRead(false);
            isInboundBlocked = true;
        }

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOGGER.trace("INACTIVE on inbound {}", ctx.channel());
        if (outboundCtx == null) {
            return;
        }
        final Channel outboundChannel = outboundCtx.channel();
        if (outboundChannel != null) {
            closeWithNoResponse(outboundChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.warn("Netty caught exception from the frontend: {}", cause.getMessage(), cause);
        toFailedFromDownstreamException(cause);
    }

    void closeInboundWithNoResponse() {
        closeWith(inboundCtx.channel(), null);
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    void closeInboundWith(@Nullable ResponseFrame response) {
        closeWith(inboundCtx.channel(), response);
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    static void closeWithNoResponse(Channel ch) {
        closeWith(ch, null);
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    static void closeWith(Channel ch, @Nullable ResponseFrame response) {
        if (ch.isActive()) {
            ch.writeAndFlush(response != null ? response : Unpooled.EMPTY_BUFFER)
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof SniCompletionEvent sniCompletionEvent) {
            if (sniCompletionEvent.isSuccess()) {
                this.sniHostname = sniCompletionEvent.hostname();
            }
            // TODO handle the failure case
        }
        else if (event instanceof AuthenticationEvent) {
            this.authentication = (AuthenticationEvent) event;
        }
        super.userEventTriggered(ctx, event);
    }

    @Override
    public String clientHost() {
        if (haProxyMessage != null) {
            return haProxyMessage.sourceAddress();
        }
        else {
            SocketAddress socketAddress = inboundCtx.channel().remoteAddress();
            if (socketAddress instanceof InetSocketAddress) {
                return ((InetSocketAddress) socketAddress).getAddress().getHostAddress();
            }
            else {
                return String.valueOf(socketAddress);
            }
        }
    }

    @Override
    public int clientPort() {
        if (haProxyMessage != null) {
            return haProxyMessage.sourcePort();
        }
        else {
            SocketAddress socketAddress = inboundCtx.channel().remoteAddress();
            if (socketAddress instanceof InetSocketAddress) {
                return ((InetSocketAddress) socketAddress).getPort();
            }
            else {
                return -1;
            }
        }
    }

    @Override
    public SocketAddress srcAddress() {
        return inboundCtx.channel().remoteAddress();
    }

    @Override
    public SocketAddress localAddress() {
        return inboundCtx.channel().localAddress();
    }

    @Override
    public String authorizedId() {
        return authentication != null ? authentication.authorizationId() : null;
    }

    @Override
    public String clientSoftwareName() {
        return clientSoftwareName;
    }

    @Override
    public String clientSoftwareVersion() {
        return clientSoftwareVersion;
    }

    @Override
    public String sniHostname() {
        return sniHostname;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.inboundCtx = ctx;
        LOGGER.trace("{}: channelActive", inboundCtx.channel().id());
        // Initially the channel is not auto reading, so read the first batch of requests
        ctx.channel().config().setAutoRead(false);
        ctx.channel().read();
        super.channelActive(ctx);
    }

    @Override
    public String toString() {
        return "KafkaProxyFrontendHandler{inbound = " + inboundCtx.channel() + ", state = " + state + "}";
    }

    /**
     * Called when the channel to the upstream broker is ready for sending KRPC requests.
     */
    private void onUpstreamChannelUsable() {
        if (state != State.CONNECTED && state != State.NEGOTIATING_TLS) {
            throw illegalState(null);
        }
        LOGGER.trace("{}: onUpstreamChannelUsable: {}", inboundCtx.channel().id(), outboundCtx.channel().id());
        // connection is complete, so first forward the buffered message
        for (Object bufferedMsg : bufferedMsgs) {
            forwardOutbound(outboundCtx, bufferedMsg);
        }
        bufferedMsgs = null; // don't pin in memory once we no longer need it
        if (pendingReadComplete) {
            pendingReadComplete = false;
            channelReadComplete(outboundCtx);
        }
        state = State.OUTBOUND_ACTIVE;

        var inboundChannel = this.inboundCtx.channel();
        // once buffered message has been forwarded we enable auto-read to start accepting further messages
        inboundChannel.config().setAutoRead(true);
    }

    /**
     * Called when the upstream channel (to the broker) is active and TLS negotiation is starting.
     * @param sslContext
     */
    private void startUpstreamTlsNegotiation(SslContext sslContext) {
        this.state = State.NEGOTIATING_TLS;
        final HostPort remote = (HostPort) outboundCtx.channel().attr(UPSTREAM_PEER_KEY).get();
        final SslHandler sslHandler = this.outboundCtx.pipeline().get(SslHandler.class);
        sslHandler.handshakeFuture().addListener(handshakeFuture -> {
            if (handshakeFuture.isSuccess()) {
                this.onUpstreamChannelUsable();
            }
            else {
                this.onUpstreamChannelFailed(remote, handshakeFuture.cause());
            }
        });
    }

}
