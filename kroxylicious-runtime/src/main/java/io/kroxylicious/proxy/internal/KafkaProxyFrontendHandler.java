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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseDataJsonConverter;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeDelegationTokenRequestData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EnvelopeRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.StopReplicaRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.UnregisterBrokerRequestData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AllocateProducerIdsRequest;
import org.apache.kafka.common.requests.AlterClientQuotasRequest;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest;
import org.apache.kafka.common.requests.AlterUserScramCredentialsRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BrokerHeartbeatRequest;
import org.apache.kafka.common.requests.BrokerRegistrationRequest;
import org.apache.kafka.common.requests.ControlledShutdownRequest;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateDelegationTokenRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeClientQuotasRequest;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeDelegationTokenRequest;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsRequest;
import org.apache.kafka.common.requests.ElectLeadersRequest;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaderAndIsrRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetDeleteRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RenewDelegationTokenRequest;
import org.apache.kafka.common.requests.StopReplicaRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.UnregisterBrokerRequest;
import org.apache.kafka.common.requests.UpdateFeaturesRequest;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
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
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.codec.DecodePredicate;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

public class KafkaProxyFrontendHandler
        extends ChannelInboundHandlerAdapter
        implements NetFilter.NetFilterContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyFrontendHandler.class);

    /** Cache ApiVersions response which we use when returning ApiVersions ourselves */
    private static final ApiVersionsResponseData API_VERSIONS_RESPONSE;
    public static final short UNKNOWN_SERVER_ERROR_CODE = (short) -1;

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

    private final Map<Class<? extends Exception>, Function<Throwable, Optional<?>>> responsesByExceptionType;

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
        /** The connection to the outbound cluster failed */
        FAILED
    }

    /**
     * The current state.
     * Transitions:
     * <code><pre>
     *    START ──→ HA_PROXY ──→ API_VERSIONS ─╭─→ CONNECTING ──→ CONNECTED ──→ OUTBOUND_ACTIVE
     *      ╰──────────╰──────────────╰────────╯        |
     *                                                  |
     *                                                  ╰──→ FAILED
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
        responsesByExceptionType = new ConcurrentHashMap<>();
    }

    @VisibleForTesting
    void registerExceptionResponse(Class<? extends Exception> exceptionClass, Function<Throwable, Optional<?>> responseFunction) {
        responsesByExceptionType.put(exceptionClass, responseFunction);
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

    public void outboundChannelActive(ChannelHandlerContext ctx) {
        this.outboundCtx = ctx;
    }

    public void outboundChannelUsable() {
        if (state != State.CONNECTED) {
            throw illegalState(null);
        }
        LOGGER.trace("{}: outboundChannelActive", inboundCtx.channel().id());
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

    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        // this is key to propagate back-pressure changes
        if (backendHandler != null) {
            backendHandler.inboundChannelWritabilityChanged(ctx);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
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

    private void bufferMessage(Object msg) {
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
        AtomicReference<Future<Channel>> sslHandshakeFuture = new AtomicReference<>();
        Channel outboundChannel = tcpConnectFuture.channel();
        ChannelPipeline pipeline = outboundChannel.pipeline();

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

        virtualCluster.getUpstreamSslContext().ifPresent(c -> {
            final SslHandler handler = c.newHandler(outboundChannel.alloc(), remote.host(), remote.port());
            pipeline.addFirst("ssl", handler);
            sslHandshakeFuture.set(handler.handshakeFuture());
            registerExceptionResponse(SSLHandshakeException.class, throwable -> {
                Object result;
                final Object triggerMsg = bufferedMsgs != null ? bufferedMsgs.get(0) : null;
                if (triggerMsg instanceof final DecodedRequestFrame<?> triggerFrame) {
                    result = buildErrorResponseFrame(triggerFrame, new UnknownServerException());
                }
                else {
                    result = null;
                }
                return Optional.ofNullable(result);
            });
        });
        // If we have an SSL future we want to wait for completion of the handshake before declaring the channel active.
        final Future<Channel> handshakeFuture = sslHandshakeFuture.get();
        Future<?> kafkaAvailableFuture = handshakeFuture != null ? handshakeFuture : tcpConnectFuture;
        kafkaAvailableFuture.addListener(future -> {
            if (future.isSuccess()) {
                onConnection(filters);
            }
            else {
                onConnectionFailed(remote, future, inboundChannel);
            }
        });
    }

    private @NonNull Object buildErrorResponseFrame(DecodedRequestFrame<?> triggerFrame, Throwable error) {
        var responseData = errorResponse(triggerFrame, error);
        final ResponseHeaderData responseHeaderData = new ResponseHeaderData();
        responseHeaderData.setCorrelationId(triggerFrame.correlationId());
        return new DecodedResponseFrame<>(triggerFrame.apiVersion(), triggerFrame.correlationId(), responseHeaderData, responseData);
    }

    @VisibleForTesting
    void onConnectionFailed(HostPort remote, Future<?> future, Channel inboundChannel) {
        state = State.FAILED;
        // Close the connection if the connection attempt has failed.
        Throwable failureCause = future.cause();
        handleException(failureCause).ifPresentOrElse(
                result -> closeWith(inboundChannel, result),
                () -> {
                    LOGGER.atWarn()
                            .setCause(LOGGER.isDebugEnabled() ? failureCause : null)
                            .log("Connection to target cluster on {} failed with: {}, closing inbound channel. Increase log level to DEBUG for stacktrace",
                                    remote, failureCause.getMessage());
                    closeOnFlush(inboundChannel);
                });
    }

    @SuppressWarnings("java:S1452")
    Optional<?> handleException(Throwable throwable) {
        var localCause = throwable;
        while (localCause != null) {
            if (responsesByExceptionType.containsKey(localCause.getClass())) {
                return responsesByExceptionType.get(localCause.getClass()).apply(localCause);
            }
            localCause = localCause.getCause();
        }
        return Optional.empty();
    }

    private void onConnection(List<FilterAndInvoker> filters) {
        state = State.CONNECTED;
        LOGGER.trace("{}: Outbound connected", inboundCtx.channel().id());
        // Now we know which filters are to be used we need to update the DecodePredicate
        // so that the decoder starts decoding the messages that the filters want to intercept
        dp.setDelegate(DecodePredicate.forFilters(filters));
        outboundChannelUsable();
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
            closeOnFlush(outboundChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.warn("Netty caught exception from the frontend: {}", cause.getMessage(), cause);
        if (cause instanceof DecoderException de && de.getCause() instanceof FrameOversizedException e) {
            var tlsHint = virtualCluster.getDownstreamSslContext().isPresent() ? "" : " or an unexpected TLS handshake";
            LOGGER.warn(
                    "Received over-sized frame, max frame size bytes {}, received frame size bytes {} "
                            + "(hint: are we decoding a Kafka frame, or something unexpected like an HTTP request{}?)",
                    e.getMaxFrameSizeBytes(), e.getReceivedFrameSizeBytes(), tlsHint);
        }
        closeOnFlush(ctx.channel());
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    void closeOnFlush(Channel ch) {
        closeWith(ch, Unpooled.EMPTY_BUFFER);
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    void closeWith(Channel ch, Object response) {
        if (ch.isActive()) {
            ch.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
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

    private ApiMessage errorResponse(DecodedRequestFrame<?> frame, Throwable error) {
        /*
         * This monstrosity is needed because there isn't any _nicely_ abstracted code we can borrow from Kafka
         * which creates and response with error codes set appropriately.
         */
        final AbstractRequest req;
        ApiMessage reqBody = frame.body();
        short apiVersion = frame.apiVersion();
        switch (frame.apiKey()) {
            case SASL_HANDSHAKE:
            case SASL_AUTHENTICATE:
                // These should have been handled by our caller
                throw new IllegalStateException();
            case PRODUCE:
                req = new ProduceRequest((ProduceRequestData) reqBody, apiVersion);
                break;
            case FETCH:
                req = new FetchRequest((FetchRequestData) reqBody, apiVersion);
                break;
            case LIST_OFFSETS:
                ListOffsetsRequestData listOffsetsRequestData = (ListOffsetsRequestData) reqBody;
                if (listOffsetsRequestData.replicaId() == ListOffsetsRequest.CONSUMER_REPLICA_ID) {
                    req = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.forId(listOffsetsRequestData.isolationLevel()), true)
                            .build(apiVersion);
                }
                else {
                    req = ListOffsetsRequest.Builder.forReplica(apiVersion, listOffsetsRequestData.replicaId())
                            .build(apiVersion);
                }
                break;
            case METADATA:
                req = new MetadataRequest((MetadataRequestData) reqBody, apiVersion);
                break;
            case OFFSET_COMMIT:
                req = new OffsetCommitRequest((OffsetCommitRequestData) reqBody, apiVersion);
                break;
            case OFFSET_FETCH:
                OffsetFetchRequestData offsetFetchRequestData = (OffsetFetchRequestData) reqBody;
                if (offsetFetchRequestData.groups() != null) {
                    req = new OffsetFetchRequest.Builder(
                            offsetFetchRequestData.groups().stream().collect(Collectors.toMap(
                                    OffsetFetchRequestData.OffsetFetchRequestGroup::groupId,
                                    x -> x.topics().stream().flatMap(
                                            t -> t.partitionIndexes().stream().map(
                                                    p -> new TopicPartition(t.name(), p)))
                                            .collect(Collectors.toList()))),
                            true, false)
                            .build(apiVersion);
                }
                else if (offsetFetchRequestData.topics() != null) {
                    req = new OffsetFetchRequest.Builder(
                            offsetFetchRequestData.groupId(),
                            offsetFetchRequestData.requireStable(),
                            offsetFetchRequestData.topics().stream().flatMap(
                                    x -> x.partitionIndexes().stream().map(
                                            p -> new TopicPartition(x.name(), p)))
                                    .collect(Collectors.toList()),
                            false)
                            .build(apiVersion);
                }
                else {
                    throw new IllegalStateException();
                }
                break;
            case FIND_COORDINATOR:
                req = new FindCoordinatorRequest.Builder((FindCoordinatorRequestData) reqBody)
                        .build(apiVersion);
                break;
            case JOIN_GROUP:
                req = new JoinGroupRequest((JoinGroupRequestData) reqBody, apiVersion);
                break;
            case HEARTBEAT:
                req = new HeartbeatRequest.Builder((HeartbeatRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LEAVE_GROUP:
                LeaveGroupRequestData data = (LeaveGroupRequestData) reqBody;
                req = new LeaveGroupRequest.Builder(data.groupId(), data.members())
                        .build(apiVersion);
                break;
            case SYNC_GROUP:
                req = new SyncGroupRequest((SyncGroupRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_GROUPS:
                req = new DescribeGroupsRequest.Builder((DescribeGroupsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LIST_GROUPS:
                req = new ListGroupsRequest((ListGroupsRequestData) reqBody, apiVersion);
                break;
            case API_VERSIONS:
                req = new ApiVersionsRequest((ApiVersionsRequestData) reqBody, apiVersion);
                break;
            case CREATE_TOPICS:
                req = new CreateTopicsRequest((CreateTopicsRequestData) reqBody, apiVersion);
                break;
            case DELETE_TOPICS:
                req = new DeleteTopicsRequest.Builder((DeleteTopicsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DELETE_RECORDS:
                req = new DeleteRecordsRequest.Builder((DeleteRecordsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case INIT_PRODUCER_ID:
                req = new InitProducerIdRequest.Builder((InitProducerIdRequestData) reqBody)
                        .build(apiVersion);
                break;
            case OFFSET_FOR_LEADER_EPOCH:
                req = new OffsetsForLeaderEpochRequest((OffsetForLeaderEpochRequestData) reqBody, apiVersion);
                break;
            case ADD_PARTITIONS_TO_TXN:
                req = new AddPartitionsToTxnRequest((AddPartitionsToTxnRequestData) reqBody, apiVersion);
                break;
            case ADD_OFFSETS_TO_TXN:
                req = new AddOffsetsToTxnRequest((AddOffsetsToTxnRequestData) reqBody, apiVersion);
                break;
            case END_TXN:
                req = new EndTxnRequest.Builder((EndTxnRequestData) reqBody)
                        .build(apiVersion);
                break;
            case WRITE_TXN_MARKERS:
                req = new WriteTxnMarkersRequest.Builder((WriteTxnMarkersRequestData) reqBody)
                        .build(apiVersion);
                break;
            case TXN_OFFSET_COMMIT:
                req = new TxnOffsetCommitRequest((TxnOffsetCommitRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_ACLS:
                DescribeAclsRequestData d = (DescribeAclsRequestData) reqBody;
                req = new DescribeAclsRequest.Builder(new AclBindingFilter(
                        new ResourcePatternFilter(
                                ResourceType.fromCode(d.resourceTypeFilter()),
                                d.resourceNameFilter(),
                                PatternType.fromCode(d.patternTypeFilter())),
                        new AccessControlEntryFilter(
                                d.principalFilter(),
                                d.hostFilter(),
                                AclOperation.fromCode(d.operation()),
                                AclPermissionType.fromCode(d.permissionType()))))
                        .build(apiVersion);
                break;
            case CREATE_ACLS:
                req = new CreateAclsRequest.Builder((CreateAclsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DELETE_ACLS:
                req = new DeleteAclsRequest.Builder((DeleteAclsRequestData) reqBody).build(apiVersion);
                break;
            case DESCRIBE_CONFIGS:
                req = new DescribeConfigsRequest((DescribeConfigsRequestData) reqBody, apiVersion);
                break;
            case ALTER_CONFIGS:
                req = new AlterConfigsRequest((AlterConfigsRequestData) reqBody, apiVersion);
                break;
            case ALTER_REPLICA_LOG_DIRS:
                req = new AlterReplicaLogDirsRequest((AlterReplicaLogDirsRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_LOG_DIRS:
                req = new DescribeLogDirsRequest((DescribeLogDirsRequestData) reqBody, apiVersion);
                break;
            case CREATE_PARTITIONS:
                req = new CreatePartitionsRequest.Builder((CreatePartitionsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case CREATE_DELEGATION_TOKEN:
                req = new CreateDelegationTokenRequest.Builder((CreateDelegationTokenRequestData) reqBody)
                        .build(apiVersion);
                break;
            case RENEW_DELEGATION_TOKEN:
                req = new RenewDelegationTokenRequest.Builder((RenewDelegationTokenRequestData) reqBody)
                        .build(apiVersion);
                break;
            case EXPIRE_DELEGATION_TOKEN:
                req = new ExpireDelegationTokenRequest.Builder((ExpireDelegationTokenRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DESCRIBE_DELEGATION_TOKEN:
                DescribeDelegationTokenRequestData tokenRequestData = (DescribeDelegationTokenRequestData) reqBody;
                req = new DescribeDelegationTokenRequest.Builder(
                        tokenRequestData.owners().stream().map(o -> new KafkaPrincipal(o.principalType(), o.principalName())).collect(Collectors.toList()))
                        .build(apiVersion);
                break;
            case DELETE_GROUPS:
                req = new DeleteGroupsRequest((DeleteGroupsRequestData) reqBody, apiVersion);
                break;
            case ELECT_LEADERS:
                ElectLeadersRequestData electLeaders = (ElectLeadersRequestData) reqBody;
                req = new ElectLeadersRequest.Builder(
                        ElectionType.valueOf(electLeaders.electionType()),
                        electLeaders.topicPartitions().stream().flatMap(
                                t -> t.partitions().stream().map(
                                        p -> new TopicPartition(t.topic(), p)))
                                .collect(Collectors.toList()),
                        electLeaders.timeoutMs())
                        .build(apiVersion);
                break;
            case INCREMENTAL_ALTER_CONFIGS:
                req = new IncrementalAlterConfigsRequest((IncrementalAlterConfigsRequestData) reqBody, apiVersion);
                break;
            case ALTER_PARTITION_REASSIGNMENTS:
                req = new AlterPartitionReassignmentsRequest.Builder((AlterPartitionReassignmentsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LIST_PARTITION_REASSIGNMENTS:
                req = new ListPartitionReassignmentsRequest.Builder((ListPartitionReassignmentsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case OFFSET_DELETE:
                req = new OffsetDeleteRequest((OffsetDeleteRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_CLIENT_QUOTAS:
                req = new DescribeClientQuotasRequest((DescribeClientQuotasRequestData) reqBody, apiVersion);
                break;
            case ALTER_CLIENT_QUOTAS:
                req = new AlterClientQuotasRequest((AlterClientQuotasRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_USER_SCRAM_CREDENTIALS:
                req = new DescribeUserScramCredentialsRequest.Builder((DescribeUserScramCredentialsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case ALTER_USER_SCRAM_CREDENTIALS:
                req = new AlterUserScramCredentialsRequest.Builder((AlterUserScramCredentialsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DESCRIBE_QUORUM:
                req = new DescribeClientQuotasRequest((DescribeClientQuotasRequestData) reqBody, apiVersion);
                break;
            case ALTER_PARTITION:
                req = new AlterPartitionRequest((AlterPartitionRequestData) reqBody, apiVersion);
                break;
            case UPDATE_FEATURES:
                req = new UpdateFeaturesRequest((UpdateFeaturesRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_CLUSTER:
                req = new DescribeClusterRequest((DescribeClusterRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_PRODUCERS:
                req = new DescribeProducersRequest.Builder((DescribeProducersRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DESCRIBE_TRANSACTIONS:
                req = new DescribeTransactionsRequest.Builder((DescribeTransactionsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LIST_TRANSACTIONS:
                req = new ListTransactionsRequest.Builder((ListTransactionsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case ALLOCATE_PRODUCER_IDS:
                req = new AllocateProducerIdsRequest((AllocateProducerIdsRequestData) reqBody, apiVersion);
                break;
            case VOTE:
                req = new VoteRequest.Builder((VoteRequestData) reqBody)
                        .build(apiVersion);
                break;
            case BEGIN_QUORUM_EPOCH:
                req = new BeginQuorumEpochRequest.Builder((BeginQuorumEpochRequestData) reqBody)
                        .build(apiVersion);
                break;
            case END_QUORUM_EPOCH:
                req = new EndQuorumEpochRequest.Builder((EndQuorumEpochRequestData) reqBody)
                        .build(apiVersion);
                break;
            case ENVELOPE:
                req = new EnvelopeRequest((EnvelopeRequestData) reqBody, apiVersion);
                break;
            case FETCH_SNAPSHOT:
                req = new FetchSnapshotRequest((FetchSnapshotRequestData) reqBody, apiVersion);
                break;
            case LEADER_AND_ISR:
                LeaderAndIsrRequestData lisr = (LeaderAndIsrRequestData) reqBody;
                req = new LeaderAndIsrRequest.Builder(apiVersion, lisr.controllerId(),
                        lisr.controllerEpoch(), lisr.brokerEpoch(),
                        lisr.ungroupedPartitionStates(),
                        lisr.topicStates().stream().collect(Collectors.toMap(
                                LeaderAndIsrRequestData.LeaderAndIsrTopicState::topicName,
                                LeaderAndIsrRequestData.LeaderAndIsrTopicState::topicId)),
                        lisr.liveLeaders().stream().map(
                                x -> new Node(
                                        x.brokerId(),
                                        x.hostName(),
                                        x.port()))
                                .collect(Collectors.toList()))
                        .build(apiVersion);
                break;
            case STOP_REPLICA:
                StopReplicaRequestData stopReplica = (StopReplicaRequestData) reqBody;
                req = new StopReplicaRequest.Builder(apiVersion,
                        stopReplica.controllerId(),
                        stopReplica.controllerEpoch(),
                        stopReplica.brokerEpoch(),
                        stopReplica.deletePartitions(),
                        stopReplica.topicStates())
                        .build(apiVersion);
                break;
            case UPDATE_METADATA:
                req = new UpdateFeaturesRequest((UpdateFeaturesRequestData) reqBody, apiVersion);
                break;
            case CONTROLLED_SHUTDOWN:
                req = new ControlledShutdownRequest.Builder((ControlledShutdownRequestData) reqBody, apiVersion)
                        .build(apiVersion);
                break;
            case BROKER_REGISTRATION:
                req = new BrokerRegistrationRequest((BrokerRegistrationRequestData) reqBody, apiVersion);
                break;
            case BROKER_HEARTBEAT:
                req = new BrokerHeartbeatRequest((BrokerHeartbeatRequestData) reqBody, apiVersion);
                break;
            case UNREGISTER_BROKER:
                req = new UnregisterBrokerRequest((UnregisterBrokerRequestData) reqBody, apiVersion);
                break;
            default:
                throw new IllegalStateException();
        }
        return req.getErrorResponse(error).data();
    }
}
