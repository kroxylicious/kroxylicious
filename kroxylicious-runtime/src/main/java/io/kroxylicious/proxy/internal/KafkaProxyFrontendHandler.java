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
import java.util.Objects;

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

    private final boolean logNetwork;
    private final boolean logFrames;
    private final VirtualCluster virtualCluster;
    private final NetFilter filter;
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
    State state = null;

    /**
     * The current state.
     * Transitions:
     * <code><pre>
     *    Start ──→ HA_PROXY ──→ API_VERSIONS ─╭─→ CONNECTING ──→ CONNECTED ────→ OUTBOUND_ACTIVE
     *      ╰──────────╰───────────────────────╯        │             │                      ↑
     *                                                  │             │                      │
     *                                                  │             ╰──→ NEGOTIATING_TLS ──╯
     *                                                  │                      │
     *                                                  ╰──→ FAILED ←──────────╯
     * </pre></code>
     * Unexpected state transitions and exceptions also cause a
     * transition to {@link State#FAILED} (via {@link #illegalState(String)}}
     */
    @VisibleForTesting
    sealed interface State permits Start, HaProxy, ApiVersions, Connecting,
            Connected, OutboundActive, NegotiatingTls, Failed {
        @NonNull
        ChannelHandlerContext inboundCtx();

        @Nullable
        HAProxyMessage haProxyMessage();

        default @Nullable ChannelHandlerContext outboundCtx() {
            // throw new IllegalStateException();
            return null;
        }

        default State bufferMessage(Object msg) {
            throw inIllegalState();
        }

        default @NonNull List<Object> bufferedMsgs() {
            return List.of();
        }

        default @Nullable String clientSoftwareName() {
            throw inIllegalState();
        }

        default @Nullable String clientSoftwareVersion() {
            throw inIllegalState();
        }

        @NonNull
        private IllegalStateException inIllegalState() {
            return new IllegalStateException("in illegal state: " + this);
        }

        @NonNull
        private IllegalStateException illegalTransition() {
            return new IllegalStateException("from: " + this);
        }

        /**
         * Transition to {@link HaProxy}
         * @return The HaProxy state
         * @throws IllegalStateException If this is not {@link Start}.
         */
        default @NonNull HaProxy toHaProxy(HAProxyMessage haProxyMessage) {
            throw illegalTransition();
        }

        /**
         * Transition to {@link ApiVersions}
         * @return The ApiVersions state
         * @throws IllegalStateException If this is not {@link Start}, or {@link HaProxy}.
         */
        default @NonNull ApiVersions toApiVersions(
                                                   DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
            throw illegalTransition();
        }

        /**
         * Transition to {@link Connecting}
         * @return The Connecting state
         * @throws IllegalStateException If this is not {@link Start}, {@link HaProxy},
         * or {@link ApiVersions}
         */
        default @NonNull Connecting toConnecting() {
            throw illegalTransition();
        }

        /**
         * Transition to {@link Connected}
         * @return The Connected state
         * @throws IllegalStateException If this is not {@link Connecting}
         */
        default @NonNull Connected toConnected(KafkaProxyFrontendHandler handler, ChannelHandlerContext outboundCtx) {
            throw illegalTransition();
        }

        /**
         * Transition to {@link NegotiatingTls}
         * @return The NegotiatingTls state
         * @throws IllegalStateException If this is not {@link Connected}
         */
        default @NonNull NegotiatingTls toNegotiatingTls(KafkaProxyFrontendHandler handler, SslContext sslContext) {
            throw illegalTransition();
        }

        /**
         * Transition to {@link OutboundActive}
         * @return The OutboundActive state
         * @throws IllegalStateException If this is not {@link Connected} or {@link NegotiatingTls}.
         */
        default @NonNull OutboundActive toOutboundActive() {
            throw illegalTransition();
        }

        /**
         * Return an error response to send to the client, or null if no response should be sent.
         * @param errorCodeEx
         * @return
         */
        default ResponseFrame errorResponse(Throwable errorCodeEx) {
            ResponseFrame errorResponse;
            var bufferedMsgs = bufferedMsgs();
            final Object triggerMsg = !bufferedMsgs.isEmpty() ? bufferedMsgs.get(0) : null;
            if (errorCodeEx != null && triggerMsg instanceof final DecodedRequestFrame<?> triggerFrame) {
                errorResponse = State.buildErrorResponseFrame(triggerFrame, errorCodeEx);
            }
            else {
                errorResponse = null;
            }
            return errorResponse;
        }

        default Failed toFailed(KafkaProxyFrontendHandler handler, Throwable errorCodeEx) {
            handler.closeInboundWith(errorResponse(errorCodeEx));
            return FAILED;
        }

        static @NonNull ResponseFrame buildErrorResponseFrame(DecodedRequestFrame<?> triggerFrame, Throwable error) {
            var responseData = KafkaProxyExceptionMapper.errorResponseMessage(triggerFrame, error);
            final ResponseHeaderData responseHeaderData = new ResponseHeaderData();
            responseHeaderData.setCorrelationId(triggerFrame.correlationId());
            return new DecodedResponseFrame<>(triggerFrame.apiVersion(), triggerFrame.correlationId(), responseHeaderData, responseData);
        }

    }

    record Start(ChannelHandlerContext inboundCtx) implements State {

        @Override
        public HAProxyMessage haProxyMessage() {
            // Impossible: Filters can't invoke FilterContext API methods
            // while in this state
            throw new IllegalStateException();
        }

        @NonNull
        @Override
        public HaProxy toHaProxy(HAProxyMessage haProxyMessage) {
            return new HaProxy(inboundCtx, haProxyMessage);
        }

        @NonNull
        @Override
        public ApiVersions toApiVersions(
                                         DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
            // TODO check the format of the strings using a regex
            // Needed to reproduce the exact behaviour for how a broker handles this
            // see org.apache.kafka.common.requests.ApiVersionsRequest#isValid()
            var clientSoftwareName = apiVersionsFrame.body().clientSoftwareName();
            var clientSoftwareVersion = apiVersionsFrame.body().clientSoftwareVersion();
            return new ApiVersions(
                    inboundCtx,
                    null,
                    clientSoftwareName,
                    clientSoftwareVersion);
        }

        @NonNull
        @Override
        public Connecting toConnecting() {
            return new Connecting(
                    inboundCtx,
                    null,
                    null,
                    null,
                    new ArrayList<>());
        }
    }

    record HaProxy(
                   @NonNull ChannelHandlerContext inboundCtx,
                   @NonNull HAProxyMessage haProxyMessage)
            implements State {
        @NonNull
        @Override
        public ApiVersions toApiVersions(DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
            // TODO check the format of the strings using a regex
            // Needed to reproduce the exact behaviour for how a broker handles this
            // see org.apache.kafka.common.requests.ApiVersionsRequest#isValid()
            var clientSoftwareName = apiVersionsFrame.body().clientSoftwareName();
            var clientSoftwareVersion = apiVersionsFrame.body().clientSoftwareVersion();
            return new ApiVersions(
                    inboundCtx,
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion);
        }

        @NonNull
        @Override
        public Connecting toConnecting() {
            return new Connecting(
                    inboundCtx,
                    haProxyMessage,
                    null,
                    null,
                    new ArrayList<>());
        }
    }

    record ApiVersions(@NonNull ChannelHandlerContext inboundCtx,
                       @Nullable HAProxyMessage haProxyMessage,
                       @Nullable String clientSoftwareName, // optional in the protocol
                       @Nullable String clientSoftwareVersion // optional in the protocol
    ) implements State {
        ApiVersions {
            Objects.requireNonNull(inboundCtx);
        }

        @NonNull
        @Override
        public Connecting toConnecting() {
            return new Connecting(
                    inboundCtx,
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    new ArrayList<>());
        }
    }

    record Connecting(@NonNull ChannelHandlerContext inboundCtx,
                      @Nullable HAProxyMessage haProxyMessage,
                      @Nullable String clientSoftwareName,
                      @Nullable String clientSoftwareVersion,
                      @NonNull List<Object> bufferedMsgs)
            implements State {
        Connecting {
            Objects.requireNonNull(inboundCtx);
            Objects.requireNonNull(bufferedMsgs);
        }

        // Messages buffered while we connect to the outbound cluster
        // The size should be limited because auto read is disabled until outbound
        // channel activation
        // private List<Object> bufferedMsgs = ;
        @NonNull
        @Override
        public Connected toConnected(KafkaProxyFrontendHandler handler, ChannelHandlerContext outboundCtx) {
            Connected connected = new Connected(
                    inboundCtx,
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    outboundCtx,
                    null);
            // TODO ugly: we need to update the handler state before we flush the bufferedMessages
            // because forwardOutbound requires the outbound connection
            // TODO But this isn't right either, because the outstream might not be active
            handler.state = connected;
            // connection is complete, so first forward the buffered message
            for (Object bufferedMsg : bufferedMsgs) {
                handler.forwardOutbound(outboundCtx, bufferedMsg);
            }
            // TODO bufferedMsgs = null; // don't pin in memory once we no longer need it
            if (handler.pendingReadComplete) {
                handler.pendingReadComplete = false;
                handler.channelReadComplete(outboundCtx);
            }
            return connected;
        }

        @Override
        public Connecting bufferMessage(Object msg) {
            this.bufferedMsgs.add(msg);
            return this;
        }
    }

    record Connected(
                     @NonNull ChannelHandlerContext inboundCtx,
                     @Nullable HAProxyMessage haProxyMessage,
                     @Nullable String clientSoftwareName,
                     @Nullable String clientSoftwareVersion,
                     @NonNull ChannelHandlerContext outboundCtx,
                     @NonNull List<Object> bufferedMsgs)
            implements State {
        Connected {
            Objects.requireNonNull(inboundCtx);
            Objects.requireNonNull(outboundCtx);
        }

        @NonNull
        @Override
        public OutboundActive toOutboundActive() {
            return new OutboundActive(
                    inboundCtx,
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    outboundCtx);
        }

        @NonNull
        @Override
        public NegotiatingTls toNegotiatingTls(KafkaProxyFrontendHandler handler, SslContext sslContext) {
            final SslHandler sslHandler = this.outboundCtx.pipeline().get(SslHandler.class);
            sslHandler.handshakeFuture().addListener(handler::onUpstreamSslOutcome);
            return new NegotiatingTls(
                    inboundCtx,
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    outboundCtx,
                    bufferedMsgs);
        }

        @Override
        public Connected bufferMessage(Object msg) {
            this.bufferedMsgs.add(msg);
            return this;
        }
    }

    record OutboundActive(
                          @NonNull ChannelHandlerContext inboundCtx,
                          @Nullable HAProxyMessage haProxyMessage,
                          @Nullable String clientSoftwareName,
                          @Nullable String clientSoftwareVersion,
                          @NonNull ChannelHandlerContext outboundCtx)
            implements State {
        OutboundActive {
            Objects.requireNonNull(inboundCtx);
            Objects.requireNonNull(outboundCtx);
        }
    }

    record NegotiatingTls(
                          @NonNull ChannelHandlerContext inboundCtx,
                          @Nullable HAProxyMessage haProxyMessage,
                          @Nullable String clientSoftwareName,
                          @Nullable String clientSoftwareVersion,
                          @NonNull ChannelHandlerContext outboundCtx,
                          List<Object> bufferedMsgs)
            implements State {
        NegotiatingTls {
            Objects.requireNonNull(inboundCtx);
            Objects.requireNonNull(outboundCtx);
        }

        @NonNull
        @Override
        public OutboundActive toOutboundActive() {
            return new OutboundActive(
                    inboundCtx,
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    outboundCtx);
        }
    }

    record Failed() implements State {
        @Override
        public ChannelHandlerContext inboundCtx() {
            return null;
        }

        @Override
        public HAProxyMessage haProxyMessage() {
            return null;
        }
    }

    static final Failed FAILED = new Failed();
    // /** The initial state */
    // START,
    // /** An HAProxy message has been received */
    // HA_PROXY,
    // /** A Kafka ApiVersions request has been received */
    // API_VERSIONS,
    // /** Some other Kafka request has been received and we're in the process of connecting to the outbound cluster */
    // CONNECTING,
    // /** The outbound connection is connected but not yet active */
    // CONNECTED,
    // /** The outbound connection is active */
    // OUTBOUND_ACTIVE,
    // /** The outbound is connected but still TLS is still being negotiated */
    // NEGOTIATING_TLS,
    // /** The connection to the outbound cluster failed */
    // FAILED
    // }

    private boolean isInboundBlocked = true;

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
        String name = state.toString();
        state = FAILED;
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
        LOGGER.trace("{}: outboundChannelActive: {}", state.inboundCtx().channel().id(), upstreamCtx.channel().id());
        this.state = state.toConnected(this, upstreamCtx);
        virtualCluster.getUpstreamSslContext().ifPresentOrElse(
                this::toNegotiatingTls,
                this::toOutboundActive);
    }

    private void onUpstreamSslOutcome(Future<? super Channel> handshakeFuture) {
        if (handshakeFuture.isSuccess()) {
            toOutboundActive();
        }
        else {
            HostPort remote = state.outboundCtx().channel().attr(UPSTREAM_PEER_KEY).get();
            toFailedDueToUpstreamConnection(remote, handshakeFuture.cause());
        }
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
        if (state instanceof OutboundActive) { // post-backend connection
            forwardOutbound(ctx, msg);
        }
        else {
            handlePreOutboundActive(ctx, msg);
        }
    }

    private void handlePreOutboundActive(ChannelHandlerContext ctx, Object msg) {
        if (state instanceof Start) {
            if (msg instanceof HAProxyMessage haProxyMessage) {
                state = state.toHaProxy(haProxyMessage);
                return;
            }
            else if (msg instanceof DecodedRequestFrame
                    && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS) {
                DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
                state = state.toApiVersions(apiVersionsFrame);
                maybeToConnecting(ctx, apiVersionsFrame);
                return;
            }
            else if (msg instanceof RequestFrame) {
                toConnecting(msg, filter);
                return;
            }
        }
        else if (state instanceof HaProxy) {
            if (msg instanceof DecodedRequestFrame
                    && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS) {
                DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
                state = state.toApiVersions(apiVersionsFrame);
                maybeToConnecting(ctx, apiVersionsFrame);
                return;
            }
            else if (msg instanceof RequestFrame) {
                toConnecting(msg, filter);
                return;
            }
        }
        else if (state instanceof ApiVersions) {
            if (msg instanceof RequestFrame) {
                toConnecting(msg, filter);
                return;
            }
        }
        else if (state instanceof Connecting connecting) {
            if (msg instanceof RequestFrame) {
                connecting.bufferMessage(msg);
                return;
            }
        }
        else if (state instanceof Connected connected) {
            if (msg instanceof RequestFrame) {
                connected.bufferMessage(msg);
                return;
            }
        }
        // TODO should be toFail
        throw illegalState("Unexpected channelRead() message of " + msg.getClass());

    }

    private void maybeToConnecting(ChannelHandlerContext ctx,
                                   DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
        if (dp.isAuthenticationOffloadEnabled()) {
            // This handler can respond to ApiVersions itself
            writeApiVersionsResponse(ctx, apiVersionsFrame);
            // Request to read the following request
            ctx.channel().read();
        }
        else {
            toConnecting(apiVersionsFrame, filter);
        }
    }

    private void toConnecting(Object msg, NetFilter filter) {
        state = state.toConnecting()
                // But for any other request we'll need a backend connection
                // (for which we need to ask the filter which cluster to connect to
                // and with what filters)
                .bufferMessage(msg);

        // TODO ensure that the filter makes exactly one upstream connection?
        // Or not for the topic routing case

        // Note filter.upstreamBroker will call back on the initiateConnect() method below
        filter.selectServer(this);
    }

    @Override
    public void initiateConnect(HostPort remote, List<FilterAndInvoker> filters) {
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
                toFailedDueToUpstreamConnection(remote, future.cause());
            }
        });
    }

    /**
     * Called when the channel to the upstream broker is failed.
     */
    @VisibleForTesting
    void toFailedDueToUpstreamConnection(HostPort remote, Throwable upstreamFailure) {
        // Close the connection if the connection attempt has failed.
        Throwable errorCodeEx;
        if (upstreamFailure instanceof SSLHandshakeException e) {
            LOGGER.atInfo()
                    .setCause(LOGGER.isDebugEnabled() ? e : null)
                    .addArgument(state.inboundCtx().channel().id())
                    .addArgument(state.outboundCtx().channel().attr(UPSTREAM_PEER_KEY).get())
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
        state = state.toFailed(this, errorCodeEx);
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
        state = state.toFailed(this, errorCodeEx);
    }

    @VisibleForTesting
    ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap b) {
        return b.connect(remoteHost, remotePort);
    }

    private void addFiltersToPipeline(List<FilterAndInvoker> filters, ChannelPipeline pipeline, Channel inboundChannel) {
        for (var filter : filters) {
            // TODO configurable timeout
            pipeline.addFirst(
                    filter.toString(),
                    new FilterHandler(
                            filter,
                            20000,
                            sniHostname,
                            virtualCluster,
                            inboundChannel));
        }
    }

    public void forwardOutbound(final ChannelHandlerContext ctx, Object msg) {
        if (state.outboundCtx() == null) {
            LOGGER.trace("READ on inbound {} ignored because outbound is not active (msg: {})",
                    ctx.channel(), msg);
            return;
        }
        final Channel outboundChannel = state.outboundCtx().channel();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("READ on inbound {} outbound {} (outbound.isWritable: {}, msg: {})",
                    ctx.channel(), outboundChannel, outboundChannel.isWritable(), msg);
            LOGGER.trace("Outbound bytesBeforeUnwritable: {}", outboundChannel.bytesBeforeUnwritable());
            LOGGER.trace("Outbound config: {}", outboundChannel.config());
            LOGGER.trace("Outbound is active, writing and flushing {}", msg);
        }
        if (outboundChannel.isWritable()) {
            outboundChannel.write(msg, state.outboundCtx().voidPromise());
            pendingFlushes = true;
        }
        else {
            outboundChannel.writeAndFlush(msg, state.outboundCtx().voidPromise());
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

    public void outboundWritabilityChanged(ChannelHandlerContext outboundCtx) {
        if (this.state.outboundCtx() != outboundCtx) {
            throw illegalState("Mismatching outboundCtx");
        }
        if (isInboundBlocked && outboundCtx.channel().isWritable()) {
            isInboundBlocked = false;
            state.inboundCtx().channel().config().setAutoRead(true);
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
        if (state.outboundCtx() == null) {
            LOGGER.trace("READ_COMPLETE on inbound {}, ignored because outbound is not active",
                    ctx.channel());
            pendingReadComplete = true;
            return;
        }
        final Channel outboundChannel = state.outboundCtx().channel();
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
        if (state == null || state.outboundCtx() == null) {
            return;
        }
        final Channel outboundChannel = state.outboundCtx().channel();
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
        closeWith(state.inboundCtx().channel(), null);
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    void closeInboundWith(@Nullable ResponseFrame response) {
        closeWith(state.inboundCtx().channel(), response);
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
        if (state.haProxyMessage() != null) {
            return state.haProxyMessage().sourceAddress();
        }
        else {
            SocketAddress socketAddress = state.inboundCtx().channel().remoteAddress();
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
        if (state.haProxyMessage() != null) {
            return state.haProxyMessage().sourcePort();
        }
        else {
            SocketAddress socketAddress = state.inboundCtx().channel().remoteAddress();
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
        return state.inboundCtx().channel().remoteAddress();
    }

    @Override
    public SocketAddress localAddress() {
        return state.inboundCtx().channel().localAddress();
    }

    @Override
    public String authorizedId() {
        return authentication != null ? authentication.authorizationId() : null;
    }

    @Override
    public String clientSoftwareName() {
        return state.clientSoftwareName();
    }

    @Override
    public String clientSoftwareVersion() {
        return state.clientSoftwareVersion();
    }

    @Override
    public String sniHostname() {
        return sniHostname;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        state = new Start(ctx);
        LOGGER.trace("{}: channelActive", state.inboundCtx().channel().id());
        // Initially the channel is not auto reading, so read the first batch of requests
        ctx.channel().config().setAutoRead(false);
        ctx.channel().read();
        super.channelActive(ctx);
    }

    @Override
    public String toString() {
        return "KafkaProxyFrontendHandler{inbound = " + state.inboundCtx().channel() + ", state = " + state + "}";
    }

    /**
     * Called when the channel to the upstream broker is ready for sending KRPC requests.
     */
    private void toOutboundActive() {
        state = state.toOutboundActive();
        LOGGER.trace("{}: onUpstreamChannelUsable: {}",
                state.inboundCtx().channel().id(),
                state.outboundCtx().channel().id());
        var inboundChannel = state.inboundCtx().channel();
        // once buffered message has been forwarded we enable auto-read to start accepting further messages
        inboundChannel.config().setAutoRead(true);
    }

    /**
     * Called when the upstream channel (to the broker) is active and TLS negotiation is starting.
     * @param sslContext
     */
    private void toNegotiatingTls(SslContext sslContext) {
        this.state = state.toNegotiatingTls(this, sslContext);
    }

}
