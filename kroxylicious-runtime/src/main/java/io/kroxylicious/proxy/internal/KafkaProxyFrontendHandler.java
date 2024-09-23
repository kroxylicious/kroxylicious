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
import java.util.List;

import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseDataJsonConverter;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;
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
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslHandler;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
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

import static io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Connecting;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Forwarding;
import static io.kroxylicious.proxy.internal.ProxyChannelState.NegotiatingTls;
import static io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;

public class KafkaProxyFrontendHandler
        extends ChannelInboundHandlerAdapter
        implements NetFilter.NetFilterContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyFrontendHandler.class);

    /** Cache ApiVersions response which we use when returning ApiVersions ourselves */
    private static final ApiVersionsResponseData API_VERSIONS_RESPONSE;
    public static final ApiException ERROR_NEGOTIATING_SSL_CONNECTION = new NetworkException("Error negotiating SSL connection");
    // public static final AttributeKey<HostPort> UPSTREAM_PEER_KEY = AttributeKey.valueOf("upstreamPeer");

    static {
        var objectMapper = new ObjectMapper();
        try (var parser = KafkaProxyFrontendHandler.class.getResourceAsStream("/ApiVersions-3.2.json")) {
            API_VERSIONS_RESPONSE = ApiVersionsResponseDataJsonConverter.read(objectMapper.readTree(parser), (short) 3);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    ChannelHandlerContext inboundCtx;
    List<Object> bufferedMsgs;

    static @NonNull ResponseFrame buildErrorResponseFrame(
                                                          @NonNull DecodedRequestFrame<?> triggerFrame,
                                                          @NonNull Throwable error) {
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
    ResponseFrame errorResponse(
                                       @NonNull ProxyChannelState state,
                                       @Nullable Throwable errorCodeEx) {
        ResponseFrame errorResponse;
        final Object triggerMsg = !bufferedMsgs.isEmpty() ? bufferedMsgs.get(0) : null;
        if (errorCodeEx != null && triggerMsg instanceof final DecodedRequestFrame<?> triggerFrame) {
            errorResponse = buildErrorResponseFrame(triggerFrame, errorCodeEx);
        }
        else {
            errorResponse = null;
        }
        return errorResponse;
    }

    private final boolean logNetwork;
    private final boolean logFrames;
    private final VirtualCluster virtualCluster;
    private final NetFilter netFilter;
    private final SaslDecodePredicate dp;

    private boolean pendingServerFlushes;
    private boolean pendingClientFlushes;
    private AuthenticationEvent authentication;
    private String sniHostname;

    // Flag if we receive a channelReadComplete() prior to outbound connection activation
    // so we can perform the channelReadComplete()/outbound flush & auto_read
    // once the outbound channel is active
    private boolean pendingReadComplete = true;

    private final StateHolder stateHolder = new StateHolder();

    private boolean isClientBlocked = true;

    KafkaProxyFrontendHandler(
                              @NonNull NetFilter netFilter,
                              @NonNull SaslDecodePredicate dp,
                              @NonNull VirtualCluster virtualCluster) {
        this.netFilter = netFilter;
        this.dp = dp;
        this.virtualCluster = virtualCluster;
        this.logNetwork = virtualCluster.isLogNetwork();
        this.logFrames = virtualCluster.isLogFrames();
    }

    @Override
    public String toString() {
        return "KafkaProxyFrontendHandler{state = " + stateHolder + "}";
    }

    @VisibleForTesting
    ProxyChannelState state() {
        return stateHolder.state();
    }

    @VisibleForTesting
    void setState(@NonNull ProxyChannelState state) {
        this.stateHolder.setState(state);
    }

    @VisibleForTesting
    void toClosed() {
        setState(new Closed());
    }

    void illegalState(@NonNull String msg) {
        stateHolder.illegalState(msg);
    }

    /**
     * Closes both upstream and downstream channels, optionally
     * sending the client the given response.
     * Called when the channel to the upstream broker is failed,
     * or TODO when either channel gets closed by the peer.
     * TODO what guarantees that the given response has the
     * type and correlation id that the client is expecting?
     */
    @VisibleForTesting
    void closeServerAndClientChannels(
                                      @Nullable ResponseFrame clientResponse) {
        // Close the server connection
        ChannelHandlerContext outboundCtx = stateHolder.state().outboundCtx();
        if (outboundCtx != null) {
            Channel outboundChannel = outboundCtx.channel();
            if (outboundChannel.isActive()) {
                outboundChannel.writeAndFlush(Unpooled.EMPTY_BUFFER)
                        .addListener(ChannelFutureListener.CLOSE);
            }
        }

        // Close the client connection with any error code
        Channel inboundChannel = this.inboundCtx.channel();
        if (inboundChannel.isActive()) {
            inboundChannel.writeAndFlush(clientResponse != null ? clientResponse : Unpooled.EMPTY_BUFFER)
                    .addListener(ChannelFutureListener.CLOSE);
        }

        toClosed();
    }

    @Override
    public void userEventTriggered(
                                   @NonNull ChannelHandlerContext ctx,
                                   @NonNull Object event)
            throws Exception {
        if (event instanceof SniCompletionEvent sniCompletionEvent) {
            if (sniCompletionEvent.isSuccess()) {
                this.sniHostname = sniCompletionEvent.hostname();
            }
            // TODO handle the failure case
        }
        else if (event instanceof AuthenticationEvent authenticationEvent) {
            this.authentication = authenticationEvent;
        }
        super.userEventTriggered(ctx, event);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.inboundCtx = ctx;
        this.stateHolder.onClientActive(this);
        super.channelActive(this.inboundCtx);
    }

    void inClientActive() {
        LOGGER.trace("{}: channelActive", this.inboundCtx.channel().id());
        // Initially the channel is not auto reading, so read the first batch of requests
        this.inboundCtx.channel().config().setAutoRead(false);
        this.inboundCtx.channel().read();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOGGER.trace("INACTIVE on inbound {}", ctx.channel());
        closeServerAndClientChannels(null);
    }

    /**
     * Relieves backpressure on the <em>server</em> connection by calling
     * the {@link KafkaProxyBackendHandler#inboundChannelWritabilityChanged()}
     * @param inboundCtx The inbound context
     * @throws Exception If something went wrong
     * @see #upstreamWritabilityChanged(ChannelHandlerContext)
     */
    @Override
    public void channelWritabilityChanged(
                                          final ChannelHandlerContext inboundCtx)
            throws Exception {
        super.channelWritabilityChanged(inboundCtx);
        if (inboundCtx.channel().isWritable()) {
            this.stateHolder.onClientUnblocked();
        }
        else {
            this.stateHolder.onClientBlocked();
        }
    }

    public void inBlocked() {
        isClientBlocked = true;
        this.inboundCtx.channel().config().setAutoRead(false);
    }

    public void inUnblocked() {
        isClientBlocked = false;
        this.inboundCtx.channel().config().setAutoRead(true);
    }

    /**
     * Handles a message from the client,
     * depending on the {@link #stateHolder} and the type of message.
     *
     * {@link #channelReadComplete(ChannelHandlerContext)} will
     * be called once this method has been invoked with all
     * the messages in the current read operation.
     * @param ctx The client context
     * @param msg The message
     */
    @Override
    public void channelRead(
                            @NonNull ChannelHandlerContext ctx,
                            @NonNull Object msg) {
        stateHolder.onRequest(dp, ctx, msg);
    }

    void inApiVersions(DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
        // This handler can respond to ApiVersions itself
        writeApiVersionsResponse(this.inboundCtx, apiVersionsFrame);
        // Request to read the following request
        this.inboundCtx.channel().read();
    }

    public void inSelectingServer() {

        // Note filter.upstreamBroker will call back on the initiateConnect() method below
        netFilter.selectServer(this);
        if (!this.stateHolder.isConnecting()) {
            illegalState("NetFilter.selectServer() did not callback on NetFilterContext.initiateConnect(): filter='" + netFilter + "'");
        }
    }

    /**
     * Sends an ApiVersions response from this handler to the client
     * if the proxy is handling authentication
     * (i.e. prior to having a backend connection)
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

    /**
     * <p>Invoked when the last message read by the current read operation
     * has been consumed by {@link #channelRead(ChannelHandlerContext, Object)}.</p>
     * @param clientCtx The client context
     */
    @Override
    public void channelReadComplete(final ChannelHandlerContext clientCtx) {
        stateHolder.clientReadComplete();
    }

    /**
     * Handles an exception in downstream/client pipeline by closing both
     * channels and changing {@link #stateHolder} to {@link Closed}.
     * @param ctx The downstream context
     * @param cause The downstream exception
     * @see #upstreamExceptionCaught(ChannelHandlerContext, Throwable)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ApiException errorCodeEx;
        if (cause instanceof DecoderException de
                && de.getCause() instanceof FrameOversizedException e) {
            var tlsHint = virtualCluster.getDownstreamSslContext().isPresent() ? "" : " or an unexpected TLS handshake";
            LOGGER.warn(
                    "Received over-sized frame from the client, max frame size bytes {}, received frame size bytes {} "
                            + "(hint: are we decoding a Kafka frame, or something unexpected like an HTTP request{}?)",
                    e.getMaxFrameSizeBytes(), e.getReceivedFrameSizeBytes(), tlsHint);
            errorCodeEx = Errors.INVALID_REQUEST.exception();
        }
        else {
            LOGGER.warn("Netty caught exception from the client: {}", cause.getMessage(), cause);
            errorCodeEx = Errors.UNKNOWN_SERVER_ERROR.exception();
        }
        closeServerAndClientChannels(errorResponse(stateHolder.state(), errorCodeEx));
    }

    void closeInboundWithNoResponse() {
        Channel ch = this.inboundCtx.channel();
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER)
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    //////////// NetFilter methods

    /**
     * Accessor exposing the client host to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The client host
     * @throws IllegalStateException if {@link #stateHolder} is not {@link SelectingServer}.
     */
    @Override
    public String clientHost() {
        if (stateHolder.state() instanceof SelectingServer selectingServer) {
            if (selectingServer.haProxyMessage() != null) {
                return selectingServer.haProxyMessage().sourceAddress();
            }
            else {
                SocketAddress socketAddress = this.inboundCtx.channel().remoteAddress();
                if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
                    return inetSocketAddress.getAddress().getHostAddress();
                }
                else {
                    return String.valueOf(socketAddress);
                }
            }
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Accessor exposing the client port to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The client port.
     * @throws IllegalStateException if {@link #stateHolder} is not {@link SelectingServer}.
     */
    @Override
    public int clientPort() {
        if (stateHolder.state() instanceof SelectingServer selectingServer) {
            if (selectingServer.haProxyMessage() != null) {
                return selectingServer.haProxyMessage().sourcePort();
            }
            else {
                SocketAddress socketAddress = this.inboundCtx.channel().remoteAddress();
                if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
                    return inetSocketAddress.getPort();
                }
                else {
                    return -1;
                }
            }
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Accessor exposing the source address to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The source address.
     * @throws IllegalStateException if {@link #stateHolder} is not {@link SelectingServer}.
     */
    @Override
    public SocketAddress srcAddress() {
        if (stateHolder.isSelectingServer()) {
            return this.inboundCtx.channel().remoteAddress();
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Accessor exposing the local address to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The local address.
     * @throws IllegalStateException if {@link #stateHolder} is not {@link SelectingServer}.
     */
    @Override
    public SocketAddress localAddress() {
        if (stateHolder.isSelectingServer()) {
            return this.inboundCtx.channel().localAddress();
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Accessor exposing the authorizedId to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The authorized id, or null.
     * @throws IllegalStateException if {@link #stateHolder} is not {@link SelectingServer}.
     */
    @Override
    public String authorizedId() {
        if (stateHolder.isSelectingServer()) {
            return authentication != null ? authentication.authorizationId() : null;
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Accessor exposing the name of the client library to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The name of the client library, or null.
     * @throws IllegalStateException if {@link #stateHolder} is not {@link SelectingServer}.
     */
    @Override
    public String clientSoftwareName() {
        if (stateHolder.state() instanceof SelectingServer selectingServer) {
            return selectingServer.clientSoftwareName();
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Accessor exposing the version of the client library to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The version of the client library, or null.
     * @throws IllegalStateException if {@link #stateHolder} is not {@link SelectingServer}.
     */
    @Override
    public String clientSoftwareVersion() {
        if (stateHolder.state() instanceof SelectingServer selectingServer) {
            return selectingServer.clientSoftwareVersion();
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Accessor exposing the SNI host name to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The SNI host name, or null.
     * @throws IllegalStateException if {@link #stateHolder} is not {@link SelectingServer}.
     */
    @Override
    public String sniHostname() {
        if (stateHolder.isSelectingServer()) {
            return sniHostname;
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Initiates the connection to a server.
     * Changes {@link #stateHolder} from {@link SelectingServer} to {@link Connecting}
     * Initializes the {@code backendHandler} and configures its pipeline
     * with the given {@code filters}.
     * <p>Called by the {@link #netFilter}.</p>
     * @param remote upstream broker target
     * @param filters The protocol filters
     */
    @Override
    public void initiateConnect(
                                @NonNull HostPort remote,
                                @NonNull List<FilterAndInvoker> filters) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Connecting to backend broker {} using filters {}",
                    this.inboundCtx.channel().id(), remote, filters);
        }
        this.stateHolder.onServerSelected(remote, filters, virtualCluster);
    }

    void inConnecting(
            @NonNull HostPort remote,
            @NonNull List<FilterAndInvoker> filters, KafkaProxyBackendHandler backendHandler) {
        final Channel inboundChannel = this.inboundCtx.channel();
        // Start the upstream connection attempt.
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(inboundChannel.eventLoop())
                .channel(inboundChannel.getClass())
                .handler(backendHandler)
                .option(ChannelOption.AUTO_READ, true)
                .option(ChannelOption.TCP_NODELAY, true);

        LOGGER.trace("Connecting to outbound {}", remote);
        ChannelFuture tcpConnectFuture = initConnection(remote.host(), remote.port(), bootstrap);
        Channel outboundChannel = tcpConnectFuture.channel();
        ChannelPipeline pipeline = outboundChannel.pipeline();

        var correlationManager = new CorrelationManager();

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
                LOGGER.trace("{}: Outbound connected", this.inboundCtx.channel().id());
                // Now we know which filters are to be used we need to update the DecodePredicate
                // so that the decoder starts decoding the messages that the filters want to intercept
                dp.setDelegate(DecodePredicate.forFilters(filters));

                // This branch does not cause the transition to Connected:
                // That happens when the backend filter call #onUpstreamChannelActive(ChannelHandlerContext).
            }
            else {
                closeServerAndClientChannels(errorResponseForServerException(future.cause()));
            }
        });
    }

    /** Ugly hack used for testing */
    @VisibleForTesting
    ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap bootstrap) {
        return bootstrap.connect(remoteHost, remotePort);
    }

    @VisibleForTesting
    ResponseFrame errorResponseForServerException(
                                                  @NonNull Throwable serverException) {

        ApiException errorCodeEx;
        if (serverException instanceof SSLHandshakeException e) {
            LOGGER.atInfo()
                    .setCause(LOGGER.isDebugEnabled() ? e : null)
                    .addArgument(this.inboundCtx.channel().id())
                    // TODO what we use UPSTREAM_PEER_KEY and not `remote`??
                    .addArgument(stateHolder.state().remote())
                    .addArgument(e.getMessage())
                    .log("{}: unable to complete TLS negotiation with {} due to: {} for further details enable debug logging");
            errorCodeEx = ERROR_NEGOTIATING_SSL_CONNECTION;
        }
        else {
            LOGGER.atWarn()
                    .setCause(LOGGER.isDebugEnabled() ? serverException : null)
                    .log("Connection to target cluster on {} failed with: {}, closing inbound channel. Increase log level to DEBUG for stacktrace",
                            stateHolder.state().remote(), serverException.getMessage());
            errorCodeEx = null;
        }

        return errorResponse(stateHolder.state(), errorCodeEx);
    }

    ////////////////////////

    private void addFiltersToPipeline(
                                      List<FilterAndInvoker> filters,
                                      ChannelPipeline pipeline,
                                      Channel inboundChannel) {
        for (var protocolFilter : filters) {
            // TODO configurable timeout
            pipeline.addFirst(
                    protocolFilter.toString(),
                    new FilterHandler(
                            protocolFilter,
                            20000,
                            sniHostname,
                            virtualCluster,
                            inboundChannel));
        }
    }

    void inForwarding() {
        // connection is complete, so first forward the buffered message
        for (Object bufferedMsg : bufferedMsgs) {
            forwardToServer(stateHolder.state().outboundCtx(), bufferedMsg);
        }
        bufferedMsgs = null;

        if (pendingReadComplete) {
            pendingReadComplete = false;
            channelReadComplete(this.inboundCtx); // TODO Why is this the outbound context?
        }

        LOGGER.trace("{}: onUpstreamChannelUsable: {}",
                this.inboundCtx.channel().id(),
                stateHolder.state().outboundCtx().channel().id());
        var inboundChannel = this.inboundCtx.channel();
        // once buffered message has been forwarded we enable auto-read to start accepting further messages
        inboundChannel.config().setAutoRead(true);
    }

    /**
     * Forwards the given {@code msg} to the server
     * @param inboundCtx The inbound (client) context.
     * @param msg The message to forward.
     */
    void forwardToServer(final ChannelHandlerContext inboundCtx, Object msg) {
        stateHolder.forwardToServer(inboundCtx, msg);
    }

    void forwardToClient(Object msg) {
//        assert blockedOutboundCtx == null;
//        LOGGER.trace("Channel read {}", msg);
        final Channel inboundChannel = inboundCtx.channel();
        if (inboundChannel.isWritable()) {
            inboundChannel.write(msg, inboundCtx.voidPromise());
            pendingClientFlushes = true;
        }
        else {
            inboundChannel.writeAndFlush(msg, inboundCtx.voidPromise());
            pendingClientFlushes = false;
        }
    }

    void flushToClient() {
        final Channel inboundChannel = inboundCtx.channel();
        if (pendingClientFlushes) {
            pendingClientFlushes = false;
            inboundChannel.flush();
        }
        if (!inboundChannel.isWritable()) {
            stateHolder.onClientBlocked();
        }
    }

    ////////////// Methods called by the backend handler

    /**
     * <p>If we're {@link Connecting} changes {@link #stateHolder} to
     * {@link Forwarding} (if TLS is not configured)
     * {@link NegotiatingTls} (if TLS is configured).</p>
     *
     * <p>Called by {@link KafkaProxyBackendHandler}
     * when the outbound channel to the upstream broker becomes active.</p>
     *
     * @param upstreamCtx
     */
    void onUpstreamChannelActive(
                                 @NonNull ChannelHandlerContext upstreamCtx) {

    }

    /**
     * <p>Closes the channel to the client.</p>
     *
     * <p>Called by {@link KafkaProxyBackendHandler}
     * when the outbound channel to the upstream broker becomes active.</p>
     *
     * @param upstreamCtx
     */
    void upstreamChannelInactive(ChannelHandlerContext upstreamCtx) {
        closeServerAndClientChannels(null);
    }

    /**
     * <p>Closes the channel to the client.</p>
     *
     * <p>Called by {@link KafkaProxyBackendHandler}
     * when the outbound channel to the upstream broker becomes active.</p>
     *
     * @param upstreamCtx The upstream context
     * @param cause The exception
     *
     * @see KafkaProxyFrontendHandler#exceptionCaught(ChannelHandlerContext, Throwable)
     */
    void upstreamExceptionCaught(ChannelHandlerContext upstreamCtx, Throwable cause) {
        // If the frontEnd has an exception handler for this exception
        // it's likely to have already dealt with it.
        // So only act here if its un-expected by the front end.
        try {
            throw cause;
        }
        catch (SSLHandshakeException e) {
            // frontendHandler.onSslHandshakeException(e);
        }
        catch (Throwable t) {
            LOGGER.atWarn()
                    .setCause(LOGGER.isDebugEnabled() ? cause : null)
                    .addArgument(cause != null ? cause.getMessage() : "")
                    .log("Netty caught exception from the backend: {}. Increase log level to DEBUG for stacktrace");
            // TODO why are we asking the frontendHander to close
            // but passing it the backend channel???
        }
        finally {
            closeServerAndClientChannels(null);
        }
    }

    public void bufferMsg(Object msg) {
        bufferedMsgs.add(msg);
    }
}
