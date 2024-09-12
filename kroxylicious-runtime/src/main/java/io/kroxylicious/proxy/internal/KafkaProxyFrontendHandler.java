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
import java.util.Optional;

import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseDataJsonConverter;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
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
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;

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

import static io.kroxylicious.proxy.internal.ProxyChannelState.ApiVersions;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Connecting;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Forwarding;
import static io.kroxylicious.proxy.internal.ProxyChannelState.HaProxy;
import static io.kroxylicious.proxy.internal.ProxyChannelState.NegotiatingTls;
import static io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Start;

public class KafkaProxyFrontendHandler
        extends ChannelInboundHandlerAdapter
        implements NetFilter.NetFilterContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyFrontendHandler.class);

    /** Cache ApiVersions response which we use when returning ApiVersions ourselves */
    private static final ApiVersionsResponseData API_VERSIONS_RESPONSE;
    public static final ApiException ERROR_NEGOTIATING_SSL_CONNECTION = new NetworkException("Error negotiating SSL connection");
    public static final AttributeKey<HostPort> UPSTREAM_PEER_KEY = AttributeKey.valueOf("upstreamPeer");

    static {
        var objectMapper = new ObjectMapper();
        try (var parser = KafkaProxyFrontendHandler.class.getResourceAsStream("/ApiVersions-3.2.json")) {
            API_VERSIONS_RESPONSE = ApiVersionsResponseDataJsonConverter.read(objectMapper.readTree(parser), (short) 3);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

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
    static ResponseFrame errorResponse(
                                       @NonNull ProxyChannelState state,
                                       @Nullable Throwable errorCodeEx) {
        ResponseFrame errorResponse;
        var bufferedMsgs = state.bufferedMsgs();
        final Object triggerMsg = !bufferedMsgs.isEmpty() ? bufferedMsgs.get(0) : null;
        if (errorCodeEx != null && triggerMsg instanceof final DecodedRequestFrame<?> triggerFrame) {
            errorResponse = buildErrorResponseFrame(triggerFrame, errorCodeEx);
        }
        else {
            errorResponse = null;
        }
        return errorResponse;
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
    static void closeWith(
                          @NonNull Channel ch,
                          @Nullable ResponseFrame response) {
        // TODO this method is less that ideally type safe,
        // because `ch` might not be the client channel
        if (ch.isActive()) {
            ch.writeAndFlush(response != null ? response : Unpooled.EMPTY_BUFFER)
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    private final boolean logNetwork;
    private final boolean logFrames;
    private final VirtualCluster virtualCluster;
    private final NetFilter netFilter;
    private final SaslDecodePredicate dp;

    private KafkaProxyBackendHandler backendHandler;
    private boolean pendingFlushes;
    private AuthenticationEvent authentication;
    private String sniHostname;

    // Flag if we receive a channelReadComplete() prior to outbound connection activation
    // so we can perform the channelReadComplete()/outbound flush & auto_read
    // once the outbound channel is active
    private boolean pendingReadComplete = true;

    @VisibleForTesting
    ProxyChannelState state = null;

    private boolean isInboundBlocked = true;

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
        return "KafkaProxyFrontendHandler{inbound = " + state.inboundCtx().channel() + ", state = " + state + "}";
    }

    @VisibleForTesting
    ProxyChannelState state() {
        return state;
    }

    private void setState(@NonNull ProxyChannelState state) {
        this.state = state;
    }

    @VisibleForTesting
    void setState(@NonNull Start state) {
        assert (this.state == null) : "" + this.state;
        setState((ProxyChannelState) state);
    }

    @VisibleForTesting
    void setState(@NonNull HaProxy state) {
        assert (this.state instanceof Start) : "" + this.state;
        setState((ProxyChannelState) state);
    }

    @VisibleForTesting
    void setState(@NonNull ApiVersions state) {
        assert (this.state instanceof Start
                || this.state instanceof HaProxy) : "" + this.state;
        setState((ProxyChannelState) state);
    }

    @VisibleForTesting
    void setState(@NonNull SelectingServer state) {
        assert (this.state instanceof Start
                || this.state instanceof HaProxy
                || this.state instanceof ApiVersions) : "" + this.state;
        setState((ProxyChannelState) state);
    }

    @VisibleForTesting
    void setState(@NonNull Connecting state) {
        assert (this.state instanceof SelectingServer) : "" + this.state;
        setState((ProxyChannelState) state);
    }

    @VisibleForTesting
    void setState(@NonNull NegotiatingTls state) {
        assert (this.state instanceof Connecting) : "" + this.state;
        setState((ProxyChannelState) state);
    }

    @VisibleForTesting
    void setState(@NonNull Forwarding state) {
        assert (this.state instanceof Connecting
                || this.state instanceof NegotiatingTls) : "" + this.state;
        setState((ProxyChannelState) state);
    }

    @VisibleForTesting
    void setState(@NonNull Closed state) {
        setState((ProxyChannelState) state);
    }

    void illegalState(@NonNull String msg) {
        if (!(state instanceof Closed)) {
            IllegalStateException exception = new IllegalStateException("While in state " + state + ": " + msg);
            LOGGER.error("Illegal state, closing channels with no client response", exception);
            closeServerAndClientChannels(null);
        }
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
        ChannelHandlerContext channelHandlerContext = state.outboundCtx();
        if (channelHandlerContext != null) {
            closeWith(channelHandlerContext.channel(), null);
        }

        // Close the client connection with any error code
        closeWith(state.inboundCtx().channel(), clientResponse);

        setState(state.toClosed());
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
        setState(new Start(ctx));
        LOGGER.trace("{}: channelActive", state.inboundCtx().channel().id());
        // Initially the channel is not auto reading, so read the first batch of requests
        ctx.channel().config().setAutoRead(false);
        ctx.channel().read();
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOGGER.trace("INACTIVE on inbound {}", ctx.channel());
        if (state != null) {
            ChannelHandlerContext outboundCtx = state.outboundCtx();
            if (outboundCtx != null) {
                final Channel outboundChannel = outboundCtx.channel();
                if (outboundChannel != null) {
                    closeWithNoResponse(outboundChannel);
                }
            }
        }
    }

    /**
     * Relieves backpressure on the <em>server</em> connection by calling
     * the {@link KafkaProxyBackendHandler#inboundChannelWritabilityChanged(ChannelHandlerContext)}
     * @param inboundCtx The inbound context
     * @throws Exception If something went wrong
     * @see #upstreamWritabilityChanged(ChannelHandlerContext)
     */
    @Override
    public void channelWritabilityChanged(
                                          final ChannelHandlerContext inboundCtx)
            throws Exception {
        super.channelWritabilityChanged(inboundCtx);
        if (inboundCtx.channel().isWritable() && backendHandler != null) {
            backendHandler.inboundChannelWritabilityChanged(inboundCtx);
        }
    }

    /**
     * Relieve backpressure on the <em>client</em> connection by turning on auto-read.
     * Called by the {@link KafkaProxyBackendHandler}.
     * @param upstreamCtx The upstream context.
     * @see #channelWritabilityChanged(ChannelHandlerContext)
     */
    public void upstreamWritabilityChanged(
                                           @NonNull ChannelHandlerContext upstreamCtx) {
        if (this.state.outboundCtx() != upstreamCtx) {
            illegalState("Mismatching outbound context");
        }
        if (isInboundBlocked && upstreamCtx.channel().isWritable()) {
            isInboundBlocked = false;
            state.inboundCtx().channel().config().setAutoRead(true);
        }
    }

    /**
     * Handles a message from the client,
     * depending on the {@link #state} and the type of message.
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
        if (state instanceof Forwarding) { // post-backend connection
            forwardToServer(ctx, msg);
            return;
        }
        else if (state instanceof Start startState) {
            if (msg instanceof HAProxyMessage haProxyMessage) {
                setState(startState.toHaProxy(haProxyMessage));
                return;
            }
            else if (msg instanceof DecodedRequestFrame
                    && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS) {
                DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
                ApiVersions apiVersions = startState.toApiVersions(apiVersionsFrame);
                setState(apiVersions);
                maybeToConnecting(apiVersions.toSelectingServer(), ctx, apiVersionsFrame);
                return;
            }
            else if (msg instanceof RequestFrame) {
                toSelectingServer(startState.toSelectingServer(), msg);
                return;
            }
        }
        else if (state instanceof HaProxy haProxyState) {
            if (msg instanceof DecodedRequestFrame
                    && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS) {
                DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
                ApiVersions apiVersions = haProxyState.toApiVersions(apiVersionsFrame);
                setState(apiVersions);
                maybeToConnecting(apiVersions.toSelectingServer(), ctx, apiVersionsFrame);
                return;
            }
            else if (msg instanceof RequestFrame) {
                toSelectingServer(haProxyState.toSelectingServer(), msg);
                return;
            }
        }
        else if (state instanceof ApiVersions apiVersionsState) {
            if (msg instanceof RequestFrame) {
                toSelectingServer(apiVersionsState.toSelectingServer(), msg);
                return;
            }
        }
        else if (state instanceof SelectingServer selectingServerState) {
            if (msg instanceof RequestFrame) {
                selectingServerState.bufferMessage(msg);
                return;
            }
        }
        else if (state instanceof Connecting connectedState) {
            if (msg instanceof RequestFrame) {
                connectedState.bufferMessage(msg);
                return;
            }
        }
        else if (state instanceof NegotiatingTls connectedState) {
            if (msg instanceof RequestFrame) {
                connectedState.bufferMessage(msg);
                return;
            }
        }
        illegalState("Unexpected message received: " + (msg == null ? "null" : "message class=" + msg.getClass()));
    }

    private void toSelectingServer(
                                   @NonNull SelectingServer selectingServer,
                                   @NonNull Object msg) {
        setState(selectingServer);
        selectingServer.bufferMessage(msg);

        // Note filter.upstreamBroker will call back on the initiateConnect() method below
        netFilter.selectServer(this);
        if (!(state instanceof Connecting)) {
            illegalState("NetFilter.selectServer() did not callback on NetFilterContext.initiateConnect(): filter='" + netFilter + "'");
        }
    }

    private void maybeToConnecting(
                                   @NonNull SelectingServer selectingServer,
                                   @NonNull ChannelHandlerContext ctx,
                                   @NonNull DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
        if (dp.isAuthenticationOffloadEnabled()) {
            // This handler can respond to ApiVersions itself
            writeApiVersionsResponse(ctx, apiVersionsFrame);
            // Request to read the following request
            ctx.channel().read();
        }
        else {
            toSelectingServer(selectingServer, apiVersionsFrame);
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
     * @param ctx The client context
     */
    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
        ChannelHandlerContext channelHandlerContext = state.outboundCtx();
        if (channelHandlerContext == null) {
            LOGGER.trace("READ_COMPLETE on inbound {}, ignored because outbound is not active",
                    ctx.channel());
            pendingReadComplete = true;
            return;
        }
        final Channel outboundChannel = channelHandlerContext.channel();
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

    /**
     * Handles an exception in downstream/client pipeline by closing both
     * channels and changing {@link #state} to {@link Closed}.
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
        closeServerAndClientChannels(errorResponse(state, errorCodeEx));
    }

    void closeInboundWithNoResponse() {
        closeWith(state.inboundCtx().channel(), null);
    }

    //////////// NetFilter methods

    /**
     * Accessor exposing the client host to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The client host
     * @throws IllegalStateException if {@link #state} is not {@link SelectingServer}.
     */
    @Override
    public String clientHost() {
        if (state instanceof SelectingServer selectingServer) {
            if (selectingServer.haProxyMessage() != null) {
                return selectingServer.haProxyMessage().sourceAddress();
            }
            else {
                SocketAddress socketAddress = selectingServer.inboundCtx().channel().remoteAddress();
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
     * @throws IllegalStateException if {@link #state} is not {@link SelectingServer}.
     */
    @Override
    public int clientPort() {
        if (state instanceof SelectingServer selectingServer) {
            if (selectingServer.haProxyMessage() != null) {
                return selectingServer.haProxyMessage().sourcePort();
            }
            else {
                SocketAddress socketAddress = selectingServer.inboundCtx().channel().remoteAddress();
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
     * @throws IllegalStateException if {@link #state} is not {@link SelectingServer}.
     */
    @Override
    public SocketAddress srcAddress() {
        if (state instanceof SelectingServer selectingServer) {
            return selectingServer.inboundCtx().channel().remoteAddress();
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Accessor exposing the local address to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The local address.
     * @throws IllegalStateException if {@link #state} is not {@link SelectingServer}.
     */
    @Override
    public SocketAddress localAddress() {
        if (state instanceof SelectingServer selectingServer) {
            return selectingServer.inboundCtx().channel().localAddress();
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Accessor exposing the authorizedId to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The authorized id, or null.
     * @throws IllegalStateException if {@link #state} is not {@link SelectingServer}.
     */
    @Override
    public String authorizedId() {
        if (state instanceof SelectingServer selectingServer) {
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
     * @throws IllegalStateException if {@link #state} is not {@link SelectingServer}.
     */
    @Override
    public String clientSoftwareName() {
        if (state instanceof SelectingServer selectingServer) {
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
     * @throws IllegalStateException if {@link #state} is not {@link SelectingServer}.
     */
    @Override
    public String clientSoftwareVersion() {
        if (state instanceof SelectingServer selectingServer) {
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
     * @throws IllegalStateException if {@link #state} is not {@link SelectingServer}.
     */
    @Override
    public String sniHostname() {
        if (state instanceof SelectingServer selectingServer) {
            return sniHostname;
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Initiates the connection to a server.
     * Changes {@link #state} from {@link SelectingServer} to {@link Connecting}
     * Initializes the {@link #backendHandler} and configures its pipeline
     * with the given {@code filters}.
     * <p>Called by the {@link #netFilter}.</p>
     * @param remote upstream broker target
     * @param filters The protocol filters
     */
    @Override
    public void initiateConnect(
                                @NonNull HostPort remote,
                                @NonNull List<FilterAndInvoker> filters) {
        if (state instanceof SelectingServer selectingServerState) {
            setState(selectingServerState.toConnecting());
        }
        else {
            // TODO why do we do the state change here, rather than
            // throwing ISE like the other methods overridden from
            // NFC, then just catch ISE in selectServer?
            illegalState("NetFilter called NetFilterContext.initiateConnect() more than once: filter='" + netFilter + "'");
            return;
        }
        if (backendHandler != null) {
            throw new IllegalStateException();
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Connecting to backend broker {} using filters {}",
                    state.inboundCtx().channel().id(), remote, filters);
        }
        var correlationManager = new CorrelationManager();

        final Channel inboundChannel = state.inboundCtx().channel();

        // Start the upstream connection attempt.
        Bootstrap b = new Bootstrap();
        backendHandler = new KafkaProxyBackendHandler(this, state.inboundCtx());
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
                LOGGER.trace("{}: Outbound connected", state.inboundCtx().channel().id());
                // Now we know which filters are to be used we need to update the DecodePredicate
                // so that the decoder starts decoding the messages that the filters want to intercept
                dp.setDelegate(DecodePredicate.forFilters(filters));

                // This branch does not cause the transition to Connected:
                // That happens when the backend filter call #onUpstreamChannelActive(ChannelHandlerContext).
            }
            else {
                closeServerAndClientChannels(errorResponseForServerException(remote, future.cause()));
            }
        });
    }

    /** Ulgy hack used for testing */
    @VisibleForTesting
    ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap bootstrap) {
        return bootstrap.connect(remoteHost, remotePort);
    }

    @VisibleForTesting
    ResponseFrame errorResponseForServerException(
                                                  @NonNull HostPort remote,
                                                  @NonNull Throwable serverException) {

        ApiException errorCodeEx;
        if (serverException instanceof SSLHandshakeException e) {
            ChannelHandlerContext channelHandlerContext = state.outboundCtx();
            LOGGER.atInfo()
                    .setCause(LOGGER.isDebugEnabled() ? e : null)
                    .addArgument(state.inboundCtx().channel().id())
                    // TODO what we use UPSTREAM_PEER_KEY and not `remote`??
                    .addArgument(channelHandlerContext == null ? null : channelHandlerContext.channel().attr(UPSTREAM_PEER_KEY).get())
                    .addArgument(e.getMessage())
                    .log("{}: unable to complete TLS negotiation with {} due to: {} for further details enable debug logging");
            errorCodeEx = ERROR_NEGOTIATING_SSL_CONNECTION;
        }
        else {
            LOGGER.atWarn()
                    .setCause(LOGGER.isDebugEnabled() ? serverException : null)
                    .log("Connection to target cluster on {} failed with: {}, closing inbound channel. Increase log level to DEBUG for stacktrace",
                            remote, serverException.getMessage());
            errorCodeEx = null;
        }

        return errorResponse(state, errorCodeEx);
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

    /**
     * Called when the upstream channel (to the broker)
     * is active and TLS negotiation is starting.
     * @param upstreamCtx The server context
     * @param sslContext The SSL context for the server
     */
    private void toNegotiatingTls(
                                  @NonNull ChannelHandlerContext upstreamCtx,
                                  @NonNull SslContext sslContext) {
        if (state instanceof Connecting connectedState) {
            setState(connectedState.toNegotiatingTls(this, upstreamCtx, sslContext));
        }
        else {
            illegalState("In unexpected state for transition to NegotiatingTls");
        }
    }

    @VisibleForTesting
    void onUpstreamSslOutcome(
                              @NonNull ChannelHandlerContext outboundCtx,
                              @NonNull Future<?> handshakeFuture) {
        if (handshakeFuture.isSuccess()) {
            toForwarding(outboundCtx);
        }
        else if (state instanceof NegotiatingTls negotiatingTlsState) {
            HostPort remote = negotiatingTlsState.outboundCtx().channel().attr(UPSTREAM_PEER_KEY).get();
            closeServerAndClientChannels(errorResponseForServerException(remote, handshakeFuture.cause()));
        }
        else {
            illegalState("TLS handshake successful, but not in NegotiatingTls state");
        }
    }

    /**
     * <p>{@linkplain #forwardToServer(ChannelHandlerContext, Object) Forwards} any buffered messages to the server and
     * changes {@link #state} to {@link Forwarding}.
     * Once in {@link Forwarding},
     * {@link #channelRead(ChannelHandlerContext, Object)} will forward messages
     * from the client to the server.</p>
     * <p>Called when KRPC requests can be sent over the channel to the upstream broker:
     * ({@link #onUpstreamChannelActive(ChannelHandlerContext)} for plain TCP, or
     * {@link #onUpstreamSslOutcome(ChannelHandlerContext, Future)} for TLS).</p>
     */
    private void toForwarding(ChannelHandlerContext outboundCtx) {
        List<Object> bufferedMsgs = List.of();
        if (state instanceof Connecting connectedState) {
            bufferedMsgs = connectedState.bufferedMsgs();
            setState(connectedState.toForwarding(outboundCtx));
        }
        else if (state instanceof NegotiatingTls negotiatingTls) {
            bufferedMsgs = negotiatingTls.bufferedMsgs();
            setState(negotiatingTls.toForwarding());
        }
        else {
            illegalState("In unexpected state for transition to OutboundActive");
        }
        // connection is complete, so first forward the buffered message
        for (Object bufferedMsg : bufferedMsgs) {
            forwardToServer(state.outboundCtx(), bufferedMsg);
        }
        // TODO bufferedMsgs = null; // don't pin in memory once we no longer need it
        if (pendingReadComplete) {
            pendingReadComplete = false;
            channelReadComplete(state.outboundCtx());
        }

        LOGGER.trace("{}: onUpstreamChannelUsable: {}",
                state.inboundCtx().channel().id(),
                state.outboundCtx().channel().id());
        var inboundChannel = state.inboundCtx().channel();
        // once buffered message has been forwarded we enable auto-read to start accepting further messages
        inboundChannel.config().setAutoRead(true);
    }

    /**
     * Forwards the given {@code msg} to the server
     * @param inboundCtx The inbound (client) context.
     * @param msg The message to forward.
     */
    private void forwardToServer(final ChannelHandlerContext inboundCtx, Object msg) {
        ChannelHandlerContext outboundCtx = state.outboundCtx();
        if (outboundCtx == null) {
            LOGGER.trace("READ on inbound {} ignored because outbound is not active (msg: {})",
                    inboundCtx.channel(), msg);
            return;
        }
        final Channel outboundChannel = outboundCtx.channel();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("READ on inbound {} outbound {} (outbound.isWritable: {}, msg: {})",
                    inboundCtx.channel(), outboundChannel, outboundChannel.isWritable(), msg);
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

    ////////////// Methods called by the backend handler

    /**
     * <p>If we're {@link Connecting} changes {@link #state} to
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
        LOGGER.trace("{}: outboundChannelActive: {}", state.inboundCtx().channel().id(), upstreamCtx.channel().id());
        if (state instanceof Connecting connectingState) {
            Optional<SslContext> upstreamSslContext = virtualCluster.getUpstreamSslContext();
            if (upstreamSslContext.isPresent()) {
                toNegotiatingTls(upstreamCtx, upstreamSslContext.get());
            }
            else {
                toForwarding(upstreamCtx);
            }
        }
        else {
            illegalState("NetFilter didn't call NetFilterContext.initiateConnect(): filter='" + netFilter + "'");
        }
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
        closeInboundWithNoResponse();
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
            this.closeInboundWithNoResponse();
        }
    }
}
