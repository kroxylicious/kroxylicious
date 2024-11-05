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
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseDataJsonConverter;
import org.apache.kafka.common.message.ResponseHeaderData;
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
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslHandler;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.ResponseFrame;
import io.kroxylicious.proxy.internal.ProxyChannelState.ApiVersions;
import io.kroxylicious.proxy.internal.ProxyChannelState.ClientActive;
import io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.codec.DecodePredicate;
import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.ProxyChannelState.Connecting;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Forwarding;
import static io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;

public class KafkaProxyFrontendHandler
        extends ChannelInboundHandlerAdapter
        implements NetFilter.NetFilterContext {

    private static final String NET_FILTER_INVOKED_IN_WRONG_STATE = "NetFilterContext invoked in wrong session state";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyFrontendHandler.class);

    /** Cache ApiVersions response which we use when returning ApiVersions ourselves */
    private static final ApiVersionsResponseData API_VERSIONS_RESPONSE;

    private final boolean logNetwork;
    private final boolean logFrames;
    private final VirtualCluster virtualCluster;
    private final NetFilter netFilter;
    private final SaslDecodePredicate dp;
    private final ProxyChannelStateMachine proxyChannelStateMachine;

    private @Nullable ChannelHandlerContext clientCtx;
    @VisibleForTesting
    @Nullable
    List<Object> bufferedMsgs;
    private boolean pendingClientFlushes;
    private @Nullable AuthenticationEvent authentication;
    private @Nullable String sniHostname;

    // Flag if we receive a channelReadComplete() prior to outbound connection activation
    // so we can perform the channelReadComplete()/outbound flush & auto_read
    // once the outbound channel is active
    private boolean pendingReadComplete = true;

    private boolean isClientBlocked = true;

    static {
        var objectMapper = new ObjectMapper();
        try (var parser = KafkaProxyFrontendHandler.class.getResourceAsStream("/ApiVersions-3.2.json")) {
            API_VERSIONS_RESPONSE = ApiVersionsResponseDataJsonConverter.read(objectMapper.readTree(parser), (short) 3);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    KafkaProxyFrontendHandler(
                              @NonNull NetFilter netFilter,
                              @NonNull SaslDecodePredicate dp,
                              @NonNull VirtualCluster virtualCluster) {
        this(netFilter, dp, virtualCluster, new ProxyChannelStateMachine());
    }

    KafkaProxyFrontendHandler(
                              @NonNull NetFilter netFilter,
                              @NonNull SaslDecodePredicate dp,
                              @NonNull VirtualCluster virtualCluster,
                              @NonNull ProxyChannelStateMachine proxyChannelStateMachine) {
        this.netFilter = netFilter;
        this.dp = dp;
        this.virtualCluster = virtualCluster;
        this.proxyChannelStateMachine = proxyChannelStateMachine;
        this.logNetwork = virtualCluster.isLogNetwork();
        this.logFrames = virtualCluster.isLogFrames();
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
                + ", authentication=" + authentication
                + ", sniHostname='" + sniHostname + '\''
                + ", pendingReadComplete=" + pendingReadComplete
                + ", isClientBlocked=" + isClientBlocked
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
                                   @NonNull ChannelHandlerContext ctx,
                                   @NonNull Object event)
            throws Exception {
        if (event instanceof SniCompletionEvent sniCompletionEvent) {
            if (sniCompletionEvent.isSuccess()) {
                this.sniHostname = sniCompletionEvent.hostname();
            }
            else {
                throw new IllegalStateException("SNI failed", sniCompletionEvent.cause());
            }
        }
        else if (event instanceof AuthenticationEvent authenticationEvent) {
            this.authentication = authenticationEvent;
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
        super.channelActive(this.clientCtx);
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
                            @NonNull ChannelHandlerContext ctx,
                            @NonNull Object msg) {
        proxyChannelStateMachine.onClientRequest(dp, msg);
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
        // Initially the channel is not auto reading, so read the first batch of requests
        clientChannel.config().setAutoRead(false);
        // TODO why doesn't the initializer set autoread to false so we don't have to set it here?
        clientChannel.read();
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link ApiVersions} state.
     */
    void inApiVersions(DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
        // This handler can respond to ApiVersions itself
        writeApiVersionsResponse(clientCtx(), apiVersionsFrame);
        // Request to read the following request
        this.clientCtx().channel().read();
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link SelectingServer} state.
     */
    void inSelectingServer() {
        // Pass this as the filter context, so that
        // filter.initiateConnect() call's back on
        // our initiateConnect() method
        this.netFilter.selectServer(this);
        this.proxyChannelStateMachine
                .assertIsConnecting("NetFilter.selectServer() did not callback on NetFilterContext.initiateConnect(): filter='" + this.netFilter + "'");
    }

    /**
     * Sends an ApiVersions response from this handler to the client
     * if the proxy is handling authentication
     * (i.e. prior to having a backend connection)
     */
    private void writeApiVersionsResponse(@NonNull ChannelHandlerContext ctx,
                                          @NonNull DecodedRequestFrame<ApiVersionsRequestData> frame) {
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
        proxyChannelStateMachine.onClientException(cause, virtualCluster.getDownstreamSslContext().isPresent());
    }

    /**
     * Accessor exposing the client host to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The client host
     * @throws IllegalStateException if {@link #proxyChannelStateMachine} is not {@link SelectingServer}.
     */
    @Override
    public String clientHost() {
        final SelectingServer selectingServer = proxyChannelStateMachine
                .enforceInSelectingServer(NET_FILTER_INVOKED_IN_WRONG_STATE);
        if (selectingServer.haProxyMessage() != null) {
            return selectingServer.haProxyMessage().sourceAddress();
        }
        else {
            SocketAddress socketAddress = clientCtx().channel().remoteAddress();
            if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
                return inetSocketAddress.getAddress().getHostAddress();
            }
            else {
                return String.valueOf(socketAddress);
            }
        }
    }

    /**
     * Accessor exposing the client port to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The client port.
     * @throws IllegalStateException if {@link #proxyChannelStateMachine} is not {@link SelectingServer}.
     */
    @Override
    public int clientPort() {
        final SelectingServer selectingServer = proxyChannelStateMachine
                .enforceInSelectingServer(NET_FILTER_INVOKED_IN_WRONG_STATE);
        if (selectingServer.haProxyMessage() != null) {
            return selectingServer.haProxyMessage().sourcePort();
        }
        else {
            SocketAddress socketAddress = clientCtx().channel().remoteAddress();
            if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
                return inetSocketAddress.getPort();
            }
            else {
                return -1;
            }
        }
    }

    /**
     * Accessor exposing the source address to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The source address.
     * @throws IllegalStateException if {@link #proxyChannelStateMachine} is not {@link SelectingServer}.
     */
    @Override
    public SocketAddress srcAddress() {
        proxyChannelStateMachine.enforceInSelectingServer(NET_FILTER_INVOKED_IN_WRONG_STATE);
        return clientCtx().channel().remoteAddress();
    }

    /**
     * Accessor exposing the local address to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The local address.
     * @throws IllegalStateException if {@link #proxyChannelStateMachine} is not {@link SelectingServer}.
     */
    @Override
    public SocketAddress localAddress() {
        proxyChannelStateMachine.enforceInSelectingServer(NET_FILTER_INVOKED_IN_WRONG_STATE);
        return clientCtx().channel().localAddress();
    }

    /**
     * Accessor exposing the authorizedId to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The authorized id, or null.
     * @throws IllegalStateException if {@link #proxyChannelStateMachine} is not {@link SelectingServer}.
     */
    @Override
    public String authorizedId() {
        proxyChannelStateMachine.enforceInSelectingServer(NET_FILTER_INVOKED_IN_WRONG_STATE);
        return authentication != null ? authentication.authorizationId() : null;
    }

    /**
     * Accessor exposing the name of the client library to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The name of the client library, or null.
     * @throws IllegalStateException if {@link #proxyChannelStateMachine} is not {@link SelectingServer}.
     */
    @Override
    public String clientSoftwareName() {
        return proxyChannelStateMachine.enforceInSelectingServer(NET_FILTER_INVOKED_IN_WRONG_STATE).clientSoftwareName();
    }

    /**
     * Accessor exposing the version of the client library to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The version of the client library, or null.
     * @throws IllegalStateException if {@link #proxyChannelStateMachine} is not {@link SelectingServer}.
     */
    @Override
    public String clientSoftwareVersion() {
        return proxyChannelStateMachine.enforceInSelectingServer(NET_FILTER_INVOKED_IN_WRONG_STATE).clientSoftwareVersion();
    }

    /**
     * Accessor exposing the SNI host name to a {@link NetFilter}.
     * <p>Called by the {@link #netFilter}.</p>
     * @return The SNI host name, or null.
     * @throws IllegalStateException if {@link #proxyChannelStateMachine} is not {@link SelectingServer}.
     */
    @Override
    public String sniHostname() {
        proxyChannelStateMachine.enforceInSelectingServer(NET_FILTER_INVOKED_IN_WRONG_STATE);
        return sniHostname;
    }

    /**
     * Initiates the connection to a server.
     * Changes {@link #proxyChannelStateMachine} from {@link SelectingServer} to {@link Connecting}
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
                    clientCtx().channel().id(), remote, filters);
        }
        this.proxyChannelStateMachine.onNetFilterInitiateConnect(remote, filters, virtualCluster, netFilter);
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link Connecting} state.
     */
    void inConnecting(
                      @NonNull HostPort remote,
                      @NonNull List<FilterAndInvoker> filters,
                      KafkaProxyBackendHandler backendHandler) {
        final Channel inboundChannel = clientCtx().channel();
        // Start the upstream connection attempt.
        final Bootstrap bootstrap = configureBootstrap(backendHandler, inboundChannel);

        LOGGER.trace("Connecting to outbound {}", remote);
        ChannelFuture serverTcpConnectFuture = initConnection(remote.host(), remote.port(), bootstrap);
        Channel outboundChannel = serverTcpConnectFuture.channel();
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
            handler.setHandshakeTimeout(100, TimeUnit.MILLISECONDS);
            pipeline.addFirst("ssl", handler);
        });

        serverTcpConnectFuture.addListener(future -> {
            if (future.isSuccess()) {
                LOGGER.trace("{}: Outbound connected", clientCtx().channel().id());
                // Now we know which filters are to be used we need to update the DecodePredicate
                // so that the decoder starts decoding the messages that the filters want to intercept
                dp.setDelegate(DecodePredicate.forFilters(filters));

                // This branch does not cause the transition to Connected:
                // That happens when the backend filter call #onUpstreamChannelActive(ChannelHandlerContext).
            }
            else {
                proxyChannelStateMachine.onServerException(future.cause());
            }
        });
    }

    /** Ugly hack used for testing */
    @NonNull
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
            channelReadComplete(this.clientCtx);
        }

        if (isClientBlocked) {
            // once buffered message has been forwarded we enable auto-read to start accepting further messages
            unblockClient();
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

    private void unblockClient() {
        isClientBlocked = false;
        var inboundChannel = clientCtx().channel();
        inboundChannel.config().setAutoRead(true);
        proxyChannelStateMachine.onClientWritable();
    }

    private ChannelHandlerContext clientCtx() {
        return Objects.requireNonNull(this.clientCtx, "clientCtx was null while in state " + this.proxyChannelStateMachine.currentState());
    }

    private static @NonNull ResponseFrame buildErrorResponseFrame(
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
    private ResponseFrame errorResponse(
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
}
