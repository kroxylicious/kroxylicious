/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.filter.impl.TopicNameCacheFilter;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.routing.DirectRouting;
import io.kroxylicious.proxy.internal.routing.UpstreamClusterModel;
import io.kroxylicious.proxy.internal.subject.DefaultSubjectBuilder;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.apache.kafka.common.security.plain.internals.PlainSaslServer.PLAIN_MECHANISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Higher level test to ensure the statemachine and the channels interact as expected
 */
class ClientConnectionStateMachineEndToEndTest {
    public static final String SNI_HOSTNAME = "external.example.com";
    public static final String CLUSTER_HOST = "internal.example.org";
    public static final int CLUSTER_PORT = 9092;
    public static final HostPort CLUSTER_HOST_PORT = new HostPort(CLUSTER_HOST, CLUSTER_PORT);

    public static final HAProxyMessage HA_PROXY_MESSAGE = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
            "1.1.1.1", "2.2.2.2", 46421, 9092);
    public static final String CLIENT_SOFTWARE_NAME = "my-kafka-lib";
    public static final String CLIENT_SOFTWARE_VERSION = "1.0.0";
    private static final Duration BACKGROUND_TASK_TIMEOUT = Duration.ofSeconds(1);
    public static final KafkaSession TEST_SESSION = new KafkaSession("testSession", KafkaSessionState.NOT_AUTHENTICATED);
    public static final String DIRECT_ROUTE_NAME = "upstream";

    private EmbeddedChannel inboundChannel;
    private ChannelHandlerContext inboundCtx;
    private EmbeddedChannel outboundChannel;
    private final AtomicBoolean outboundClosed = new AtomicBoolean(false);

    int correlationId = 0;
    private KafkaProxyFrontendHandler handler;
    private KafkaProxyBackendHandler backendHandler;
    private boolean activateOutboundChannelAutomatically = true;

    ClientConnectionStateMachine clientConnectionStateMachine(EndpointBinding binding) {
        var kafkaSession = new KafkaSession(KafkaSessionState.ESTABLISHING);
        // Override createServerConnection to substitute EmbeddedChannels for real TCP connections
        return new ClientConnectionStateMachine(binding, new DefaultSubjectBuilder(List.of()), kafkaSession,
                (remote, ccsm, vc, cn, ni, connectionCounter, errorCounter, backpressureMeter, connectionToken, tlsConfig) -> new ServerConnectionStateMachine(remote,
                        ccsm,
                        vc, cn, ni, connectionCounter, errorCounter,
                        backpressureMeter, connectionToken, tlsConfig) {
                    @Override
                    Bootstrap configureBootstrap(
                                                 KafkaProxyBackendHandler capturedBackendHandler,
                                                 Channel inboundChannel) {
                        ClientConnectionStateMachineEndToEndTest.this.backendHandler = capturedBackendHandler;
                        newOutboundChannel();
                        Bootstrap bootstrap = new Bootstrap();
                        bootstrap.group(outboundChannel.eventLoop())
                                .channel(outboundChannel.getClass())
                                .handler(capturedBackendHandler)
                                .option(ChannelOption.AUTO_READ, true)
                                .option(ChannelOption.TCP_NODELAY, true);
                        return bootstrap;
                    }

                    @Override
                    ChannelFuture initConnection(
                                                 String remoteHost,
                                                 int remotePort,
                                                 Bootstrap bootstrap) {
                        outboundChannel.pipeline().addFirst(
                                ClientConnectionStateMachineEndToEndTest.this.backendHandler);
                        outboundChannel.pipeline().fireChannelRegistered();
                        if (ClientConnectionStateMachineEndToEndTest.this.activateOutboundChannelAutomatically) {
                            outboundChannel.pipeline().fireChannelActive();
                        }
                        return outboundChannel.newPromise();
                    }
                });
    }

    @AfterEach
    void closeChannel() {
        if (inboundChannel != null) {
            inboundChannel.finishAndReleaseAll();
        }
        if (outboundChannel != null) {
            outboundChannel.finishAndReleaseAll();
        }
    }

    @Test
    void toClientActive() {
        // Given
        ClientConnectionStateMachine clientConnectionStateMachine = buildFrontendHandler(false);
        assertThat(clientConnectionStateMachine.state()).isExactlyInstanceOf(ClientConnectionState.Startup.class);
        activateOutboundChannelAutomatically = false;

        // When
        hClientConnect(clientConnectionStateMachine, handler);

        // Then
        inboundChannel.checkException();
        assertThat(inboundChannel.config().isAutoRead()).isFalse();
        assertThat(inboundChannel.isWritable()).isTrue();

        assertThat(clientConnectionStateMachine.state()).isExactlyInstanceOf(ClientConnectionState.ClientActive.class);
    }

    @ParameterizedTest
    @MethodSource("clientException")
    void toClientActiveThenException(Throwable clientException) {
        // Given
        var clientConnectionStateMachine = buildHandlerInClientActiveState(false);

        // When
        handler.exceptionCaught(inboundCtx, clientException);

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse(clientConnectionStateMachine);
    }

    @Test
    void toClientActiveThenUnexpectedMessage() {
        // Given
        var clientConnectionStateMachine = buildHandlerInClientActiveState(false);

        // When
        inboundChannel.writeInbound("unexpected");

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse(clientConnectionStateMachine);
    }

    @Test
    void toClientActiveThenInactive() {
        // Given
        var clientConnectionStateMachine = buildHandlerInClientActiveState(false);

        // When
        inboundChannel.close();

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse(clientConnectionStateMachine);
    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKey")
    void clientActiveToConnecting(
                                  boolean sni,
                                  boolean haProxy,
                                  ApiKeys firstMessage) {
        // Given
        // Keeps the statemachine from automatically progressing so we can assert intermediate state
        activateOutboundChannelAutomatically = false;
        var clientConnectionStateMachine = buildHandlerInClientActiveState(sni);

        if (haProxy) {
            clientConnectionStateMachine.forceState(
                    new ClientConnectionState.HaProxy(),
                    handler,
                    java.util.Map.of(),
                    TEST_SESSION, false);
        }

        // When
        switch (firstMessage) {
            case API_VERSIONS -> writeInboundApiVersionsRequest();
            case SASL_HANDSHAKE -> writeSaslPlainHandshake();
            case SASL_AUTHENTICATE -> writeSaslAuthenticate("pa55word".getBytes(StandardCharsets.UTF_8));
            case METADATA -> writeInboundMetadataRequest();
            default -> throw new IllegalArgumentException();
        }

        // Then
        inboundChannel.checkException();
        assertThat(inboundChannel.<Object> readOutbound())
                .describedAs("No response deferred until upstream connected")
                .isNull();
        assertThat(inboundChannel.isWritable()).isTrue();
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Forwarding.class);
        assertThat(clientConnectionStateMachine.clientSoftwareName())
                .isEqualTo(firstMessage == ApiKeys.API_VERSIONS ? CLIENT_SOFTWARE_NAME : null);
        assertThat(clientConnectionStateMachine.clientSoftwareVersion())
                .isEqualTo(firstMessage == ApiKeys.API_VERSIONS ? CLIENT_SOFTWARE_VERSION : null);

        if (haProxy) {
            // forceState with transportSubjectReady=false prevents unblockClient from firing
            assertThat(inboundChannel.config().isAutoRead()).isFalse();
            assertThat(handler.bufferedMsgs)
                    .asInstanceOf(InstanceOfAssertFactories.list(DecodedRequestFrame.class))
                    .map(DecodedRequestFrame::apiKey).isEqualTo(List.of(firstMessage));
        }
        else {
            // Both latch events fire (entering Forwarding + transport subject via runPendingTasks)
            // so the client is unblocked and the request is forwarded to the SCSM
            assertThat(inboundChannel.config().isAutoRead()).isTrue();
            assertThat(handler.bufferedMsgs).isNull();
        }
    }

    private ClientConnectionStateMachine buildHandlerInClientActiveState(boolean sni) {
        var clientConnectionStateMachine = buildFrontendHandler(false);
        activateOutboundChannelAutomatically = false;

        hClientConnect(clientConnectionStateMachine, handler);
        assertThat(clientConnectionStateMachine.state()).isExactlyInstanceOf(ClientConnectionState.ClientActive.class);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }
        return clientConnectionStateMachine;
    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKey")
    void plainServerChannelActivationInConnecting(
                                                  boolean sni,
                                                  boolean haProxy,
                                                  ApiKeys firstMessage) {
        // Given
        var clientConnectionStateMachine = buildHandlerInConnectingState(sni, false, firstMessage);

        // When
        outboundChannel.pipeline().fireChannelActive();

        // Then
        inboundChannel.checkException();
        outboundChannel.checkException();
        assertThat(outboundChannel.<Object> readOutbound())
                .describedAs("Buffered request should be forwarded to server")
                .isNotNull();

        assertProxyActive(clientConnectionStateMachine);
        assertThat(handler.bufferedMsgs).isNull();
    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKeyXserverException")
    void plainServerChannelActivationThenException(
                                                   boolean sni,
                                                   boolean haProxy,
                                                   ApiKeys firstMessage,
                                                   Throwable serverException) {
        // Given
        var clientConnectionStateMachine = buildHandlerInConnectingState(sni, false, firstMessage);
        final DecodedRequestFrame<ApiMessage> requestFrame = firstInitialRequest();

        // When
        outboundChannel.pipeline().fireExceptionCaught(serverException);

        // Then
        inboundChannel.checkException();
        outboundChannel.checkException();

        assertNextClientResponseIsErrorFor(requestFrame);
        assertNoMoreResponses();
        assertEverythingClosed(clientConnectionStateMachine);
    }

    private void assertNoMoreResponses() {
        assertThat((Object) this.inboundChannel.readInbound()).isNull();
    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKey")
    void plainServerChannelActivationThenInactive(
                                                  boolean sni,
                                                  boolean haProxy,
                                                  ApiKeys firstMessage) {
        // Given
        var clientConnectionStateMachine = buildHandlerInConnectingState(sni, false, firstMessage);

        // When
        outboundChannel.pipeline().fireChannelInactive();

        // Then
        inboundChannel.checkException();
        outboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse(clientConnectionStateMachine);
    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKey")
    void tlsServerChannelActivationInConnecting(
                                                boolean sni,
                                                boolean haProxy,
                                                ApiKeys firstMessage) {
        // Given
        var clientConnectionStateMachine = buildHandlerInConnectingState(sni, true, firstMessage);

        // When
        outboundChannel.pipeline().fireChannelActive();

        // Then
        inboundChannel.checkException();

        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Forwarding.class);

        assertThat(inboundChannel.config().isAutoRead())
                .describedAs("Client is already unblocked; SCSM buffers until TLS handshake completes")
                .isTrue();
    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKey")
    void tlsHandshakeFail(
                          boolean sni,
                          boolean haProxy,
                          ApiKeys firstMessage) {
        // Given
        var clientConnectionStateMachine = buildHandlerInConnectingState(sni, true, firstMessage);
        final DecodedRequestFrame<ApiMessage> requestFrame = firstInitialRequest();
        outboundChannel.pipeline().fireChannelActive();

        // When
        outboundChannel.pipeline().fireUserEventTriggered(new SslHandshakeCompletionEvent(new SSLHandshakeException("Oops")));

        // Then
        inboundChannel.checkException();
        outboundChannel.checkException();

        assertNextClientResponseIsErrorFor(requestFrame);

        assertEverythingClosed(clientConnectionStateMachine);
    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKey")
    void tlsHandshakeSuccess(
                             boolean sni,
                             boolean haProxy,
                             ApiKeys firstMessage) {
        // Given
        var clientConnectionStateMachine = buildHandlerInConnectingState(sni, true, firstMessage);
        outboundChannel.pipeline().fireChannelActive();

        // When
        outboundChannel.pipeline().fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS);

        // Then
        inboundChannel.checkException();

        assertProxyActive(clientConnectionStateMachine);
    }

    private void assertProxyActive(ClientConnectionStateMachine clientConnectionStateMachine) {
        assertThat(clientConnectionStateMachine.state())
                .isInstanceOf(ClientConnectionState.Forwarding.class);

        assertThat(inboundChannel.config().isAutoRead())
                .describedAs("Client autoread should be on once connected to server")
                .isTrue();

        assertChannelOpen(inboundChannel);
        assertChannelOpen(outboundChannel);
    }

    private void assertChannelOpen(EmbeddedChannel channel) {
        assertThat(channel.isActive()).isTrue();
        assertThat(channel.isOpen()).isTrue();
    }

    private static DecodedRequestFrame<ApiMessage> decodedRequestFrame(
                                                                       short apiVersion,
                                                                       ApiMessage body,
                                                                       int downstreamCorrelationId) {
        var apiKey = ApiKeys.forId(body.apiKey());

        RequestHeaderData header = new RequestHeaderData()
                .setRequestApiKey(apiKey.id)
                .setRequestApiVersion(apiVersion)
                .setClientId("client-id")
                .setCorrelationId(downstreamCorrelationId);

        return new DecodedRequestFrame<>(apiVersion, downstreamCorrelationId, true, header, body);
    }

    private int writeRequest(short apiVersion, ApiMessage body) {
        int downstreamCorrelationId = correlationId++;

        DecodedRequestFrame<ApiMessage> apiMessageDecodedRequestFrame = decodedRequestFrame(apiVersion, body, downstreamCorrelationId);
        inboundChannel.writeInbound(apiMessageDecodedRequestFrame);
        return downstreamCorrelationId;
    }

    private int writeInboundApiVersionsRequest() {
        return writeRequest(ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsRequestData()
                .setClientSoftwareName(CLIENT_SOFTWARE_NAME)
                .setClientSoftwareVersion(CLIENT_SOFTWARE_VERSION));
    }

    private void writeSaslPlainHandshake() {
        writeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new SaslHandshakeRequestData()
                .setMechanism(PLAIN_MECHANISM));
    }

    private void writeSaslAuthenticate(byte[] authBytes) {
        writeRequest(SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION, new SaslAuthenticateRequestData()
                .setAuthBytes(authBytes));
    }

    private void writeInboundMetadataRequest() {
        writeRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION, new MetadataRequestData());
    }

    private KafkaProxyFrontendHandler handler(
                                              ClientConnectionStateMachine clientConnectionStateMachine,
                                              DelegatingDecodePredicate dp) {
        var pfr = mock(PluginFactoryRegistry.class);
        return new KafkaProxyFrontendHandler(pfr,
                mock(EndpointReconciler.class),
                new ApiVersionsServiceImpl(),
                dp,
                new DefaultSubjectBuilder(List.of()),
                clientConnectionStateMachine, Optional.empty());
    }

    ClientConnectionStateMachine buildFrontendHandler(boolean tlsConfigured) {
        this.inboundChannel = new EmbeddedChannel();
        this.correlationId = 0;

        var dp = new DelegatingDecodePredicate();
        VirtualClusterModel virtualClusterModel = mock(VirtualClusterModel.class);
        when(virtualClusterModel.getClusterName()).thenReturn("cluster");
        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(CacheConfiguration.DEFAULT, "cluster");
        when(virtualClusterModel.getTopicNameCacheFilter()).thenReturn(topicNameCacheFilter);
        // FCF is now resolved per-connection from the VC (see #4055). An empty FCF here is
        // sufficient — these tests don't exercise filter behavior, just connection flow.
        when(virtualClusterModel.filterChainFactory()).thenReturn(new io.kroxylicious.proxy.bootstrap.FilterChainFactory(null, java.util.List.of()));
        EndpointBinding endpointBinding = mock(EndpointBinding.class);
        EndpointGateway endpointGateway = mock(EndpointGateway.class);
        when(endpointGateway.virtualCluster()).thenReturn(virtualClusterModel);
        when(endpointBinding.endpointGateway()).thenReturn(endpointGateway);
        when(endpointBinding.upstreamTarget()).thenReturn(new HostPort(CLUSTER_HOST, CLUSTER_PORT));
        var targetCluster = new TargetCluster(CLUSTER_HOST + ":" + CLUSTER_PORT, Optional.empty());
        when(virtualClusterModel.routing()).thenReturn(new DirectRouting(DIRECT_ROUTE_NAME, targetCluster));
        final Optional<SslContext> sslContext;
        try {
            sslContext = Optional.ofNullable(tlsConfigured ? SslContextBuilder.forClient().build() : null);
        }
        catch (SSLException e) {
            throw new RuntimeException(e);
        }
        when(virtualClusterModel.getUpstreamClusterForRoute(DIRECT_ROUTE_NAME))
                .thenReturn(new UpstreamClusterModel(targetCluster, sslContext, TlsCredentialSupplierManager.unconfigured()));
        when(virtualClusterModel.getClusterName()).thenReturn("RandomCluster");
        var clientConnectionStateMachine = clientConnectionStateMachine(endpointBinding);

        this.handler = handler(clientConnectionStateMachine, dp);
        this.inboundCtx = mock(ChannelHandlerContext.class);
        when(inboundCtx.channel()).thenReturn(inboundChannel);
        when(inboundCtx.pipeline()).thenReturn(inboundChannel.pipeline());
        when(inboundCtx.handler()).thenReturn(handler);
        return clientConnectionStateMachine;
    }

    private void newOutboundChannel() {
        this.outboundChannel = new EmbeddedChannel();
        outboundChannel.closeFuture().addListener(future -> outboundClosed.set(true));
        ChannelHandlerContext outboundCtx = mock(ChannelHandlerContext.class);
        when(outboundCtx.channel()).thenReturn(outboundChannel);
        when(outboundCtx.pipeline()).thenReturn(outboundChannel.pipeline());
        when(outboundCtx.voidPromise()).thenAnswer(in -> outboundChannel.voidPromise());
        when(outboundCtx.newFailedFuture(any())).thenAnswer(in -> outboundChannel.newFailedFuture(in.getArgument(0)));
        when(outboundCtx.newSucceededFuture()).thenAnswer(in -> outboundChannel.newSucceededFuture());
    }

    // transitions from each state
    // each of the events that can happen in that state
    private void hClientConnect(ClientConnectionStateMachine clientConnectionStateMachine, KafkaProxyFrontendHandler handler) {
        final ChannelPipeline pipeline = inboundChannel.pipeline();
        if (pipeline.get(KafkaProxyFrontendHandler.class) == null) {
            pipeline.addLast(handler);
            pipeline.addLast(new FilterChainCompletionHandler(clientConnectionStateMachine));
        }
        assertThat(clientConnectionStateMachine.state()).isExactlyInstanceOf(ClientConnectionState.Startup.class);
        pipeline.fireChannelActive();
        assertThat(clientConnectionStateMachine.state()).isExactlyInstanceOf(ClientConnectionState.ClientActive.class);
    }

    private void assertNoClientResponses() {
        Object actual = inboundChannel.readOutbound();
        if (actual != null) {
            var responseAssert = assertThat(actual)
                    .describedAs("No response sent to client")
                    .asInstanceOf(InstanceOfAssertFactories.type(ByteBuf.class));
            responseAssert.extracting(ByteBuf::readableBytes).isEqualTo(0);
        }
    }

    private void assertClientConnectionClosedWithNoResponse(ClientConnectionStateMachine clientConnectionStateMachine) {
        assertNoClientResponses();

        assertThat(inboundChannel.isOpen())
                .describedAs("Connection to client is closed")
                .isFalse();

        assertEverythingClosed(clientConnectionStateMachine);
    }

    private <T extends ApiMessage> void assertClientResponse(int expectedCorrId,
                                                             Class<T> expectedResponseBodyClass,
                                                             @Nullable Function<T, Short> errorCodeFn,
                                                             @Nullable Errors expectedErrorCode) {
        if (errorCodeFn != null ^ expectedErrorCode != null) {
            throw new IllegalArgumentException("Either both, or neither, should be null");
        }
        assertThat(inboundChannel.<Object> readOutbound())
                .describedAs("Response sent to client")
                .asInstanceOf(InstanceOfAssertFactories.type(DecodedResponseFrame.class)) // asInstanceOf asserts the expected type internally
                .satisfies(decodedResponseFrame -> {
                    assertThat(decodedResponseFrame.correlationId()).isEqualTo(expectedCorrId);
                    if (errorCodeFn != null) {
                        assertThat(decodedResponseFrame.body())
                                .asInstanceOf(type(expectedResponseBodyClass))
                                .extracting(errorCodeFn)
                                .isEqualTo(expectedErrorCode.code());
                    }
                });
    }

    private void assertNextClientResponseIsApiVersionsError(int corrId) {
        assertClientResponse(
                corrId,
                ApiVersionsResponseData.class,
                ApiVersionsResponseData::errorCode,
                Errors.UNKNOWN_SERVER_ERROR);
    }

    private void assertNextClientResponseIsSaslAuthenticateError(int corrId) {
        assertClientResponse(
                corrId,
                SaslAuthenticateResponseData.class,
                SaslAuthenticateResponseData::errorCode,
                Errors.UNKNOWN_SERVER_ERROR);
    }

    private void assertNextClientResponseIsSaslHandshakeError(int corrId) {
        assertClientResponse(
                corrId,
                SaslHandshakeResponseData.class,
                SaslHandshakeResponseData::errorCode,
                Errors.UNKNOWN_SERVER_ERROR);
    }

    private void assertNextClientResponseIsMetadataError(int corrId) {
        assertClientResponse(
                corrId,
                MetadataResponseData.class,
                null,
                null);
    }

    @SafeVarargs
    static List<Arguments> crossProduct(List<Arguments>... list) {
        if (list.length == 0) {
            return List.of();
        }
        if (list.length == 1) {
            return list[0];
        }
        var l = new ArrayList<>(Arrays.asList(list));
        return crossProduct(l);
    }

    static List<Arguments> crossProduct(List<List<Arguments>> list) {
        if (list.isEmpty()) {
            return List.of();
        }
        if (list.size() == 1) {
            return list.get(0);
        }
        var first = list.remove(0);
        var rest = crossProduct(list);
        var result = new ArrayList<Arguments>();
        for (var argList : first) {
            for (var r : rest) {
                Object[] newArgs = new Object[argList.get().length + r.get().length];
                System.arraycopy(argList.get(), 0, newArgs, 0, argList.get().length);
                System.arraycopy(r.get(), 0, newArgs, argList.get().length, r.get().length);
                result.add(Arguments.of(newArgs));
            }
        }
        return result;
    }

    static List<Arguments> bool() {
        return List.of(
                Arguments.of(false),
                Arguments.of(true));
    }

    static List<Arguments> apiKey() {
        return List.of(
                Arguments.of(ApiKeys.API_VERSIONS),
                Arguments.of(ApiKeys.SASL_HANDSHAKE),
                Arguments.of(ApiKeys.SASL_AUTHENTICATE),
                Arguments.of(ApiKeys.METADATA));
    }

    static List<Arguments> serverException() {
        return List.of(
                Arguments.of(new SSLException("boom!")),
                Arguments.of(new SSLHandshakeException("boom!")),
                Arguments.of(new RuntimeException("boom!")));
    }

    static List<Arguments> clientException() {
        return List.of(
                Arguments.of(new DecoderException(new FrameOversizedException(1, 2))),
                Arguments.of(new RuntimeException("boom!")));
    }

    static List<Arguments> booleanXbooleanXapiKey() {
        return crossProduct(bool(), bool(), apiKey());
    }

    static List<Arguments> booleanXbooleanXapiKeyXserverException() {
        return crossProduct(bool(), bool(), apiKey(), serverException());
    }

    private ClientConnectionStateMachine buildHandlerInConnectingState(
                                                                       boolean sni,
                                                                       boolean tlsConfigured,
                                                                       ApiKeys firstMessage) {
        activateOutboundChannelAutomatically = false;
        var clientConnectionStateMachine = buildFrontendHandler(tlsConfigured);

        hClientConnect(clientConnectionStateMachine, handler);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }
        // Complete the transport subject building (sets transportSubjectReady = true)
        inboundChannel.runPendingTasks();

        // Write the first client request to trigger the ClientActive → Forwarding transition.
        // This creates a real SCSM and initiates the backend connection. Since
        // transportSubjectReady is already true, tryUnblockClient() fires, enabling autoRead
        // and forwarding the buffered request through the filter chain to the SCSM (which
        // buffers it in Connecting state).
        switch (firstMessage) {
            case API_VERSIONS -> writeInboundApiVersionsRequest();
            case SASL_HANDSHAKE -> writeSaslPlainHandshake();
            case SASL_AUTHENTICATE -> writeSaslAuthenticate("pa55word".getBytes(StandardCharsets.UTF_8));
            case METADATA -> writeInboundMetadataRequest();
            default -> throw new IllegalArgumentException();
        }

        return clientConnectionStateMachine;
    }

    @SuppressWarnings("unchecked")
    @NonNull
    private DecodedRequestFrame<ApiMessage> firstInitialRequest() {
        return (DecodedRequestFrame<ApiMessage>) Objects.requireNonNull(handler.initialRequestForError);
    }

    // TODO backpressure

    private void assertNextClientResponseIsErrorFor(DecodedRequestFrame<ApiMessage> requestFrame) {
        switch (requestFrame.apiKey()) {
            case API_VERSIONS -> assertNextClientResponseIsApiVersionsError(requestFrame.correlationId());
            case SASL_HANDSHAKE -> assertNextClientResponseIsSaslHandshakeError(requestFrame.correlationId());
            case SASL_AUTHENTICATE -> assertNextClientResponseIsSaslAuthenticateError(requestFrame.correlationId());
            case METADATA -> assertNextClientResponseIsMetadataError(requestFrame.correlationId());
            default -> throw new UnsupportedOperationException("unsupported apiKey: " + requestFrame.apiKeyId());
        }
    }

    private void assertEverythingClosed(ClientConnectionStateMachine clientConnectionStateMachine) {
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        if (outboundChannel != null) {
            await("outboundClosed").atMost(BACKGROUND_TASK_TIMEOUT).untilTrue(outboundClosed);
            assertThat(outboundChannel.isActive()).isFalse();
            assertThat(outboundChannel.isOpen()).isFalse();
        }
        assertThat(inboundChannel.isActive()).isFalse();
        assertThat(inboundChannel.isOpen()).isFalse();

        await("transition to closed").atMost(BACKGROUND_TASK_TIMEOUT)
                .untilAsserted(() -> assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class));
    }

    /**
     * Integration test to verify handler ordering in the complete pipeline after filters are installed.
     *
     * Critical ordering requirements:
     * - HaProxyMessageHandler (if present) must come before FilterHandler instances
     * - FilterHandler instances must come before KafkaProxyFrontendHandler
     *
     * This ensures:
     * - HAProxyMessage is intercepted before filters see it
     * - Filters process Kafka messages before the frontend handler
     * - State machine receives HAProxyMessage before any Kafka messages
     */
    @Test
    void shouldMaintainCorrectHandlerOrderingAfterFiltersInstalled() {
        // Given - Build handler which will install filters during ClientActive transition
        var clientConnectionStateMachine = buildFrontendHandler(false);

        // When - Transition to ClientActive state (this installs filters in the pipeline)
        hClientConnect(clientConnectionStateMachine, handler);

        // Then - Verify the pipeline ordering
        ChannelPipeline pipeline = inboundChannel.pipeline();
        List<String> handlerNames = pipeline.names();

        // Find positions of critical handlers
        int haProxyHandlerIndex = handlerNames.indexOf("HaProxyMessageHandler");
        int frontendHandlerIndex = findHandlerIndex(handlerNames, handler);

        // Find first filter handler (they're named "filter-N-FilterName")
        int firstFilterIndex = findFirstFilterHandler(handlerNames);

        // Assert ordering if handlers exist
        if (haProxyHandlerIndex >= 0 && firstFilterIndex >= 0) {
            assertThat(haProxyHandlerIndex)
                    .as("HaProxyMessageHandler (at index %d) must come before first filter (at index %d) in pipeline: %s",
                            haProxyHandlerIndex, firstFilterIndex, handlerNames)
                    .isLessThan(firstFilterIndex);
        }

        if (firstFilterIndex >= 0) {
            assertThat(firstFilterIndex)
                    .as("First filter (at index %d) must come after KafkaProxyFrontendHandler (at index %d) in pipeline: %s",
                            firstFilterIndex, frontendHandlerIndex, handlerNames)
                    .isGreaterThan(frontendHandlerIndex);
        }

        // Additional validation: frontend handler must be present
        assertThat(frontendHandlerIndex)
                .as("KafkaProxyFrontendHandler must be present in pipeline: %s", handlerNames)
                .isGreaterThanOrEqualTo(0);
    }

    /**
     * Find the index of the handler instance in the pipeline.
     */
    private int findHandlerIndex(List<String> handlerNames, KafkaProxyFrontendHandler handler) {
        ChannelPipeline pipeline = inboundChannel.pipeline();
        for (int i = 0; i < handlerNames.size(); i++) {
            if (pipeline.get(handlerNames.get(i)) == handler) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Find the index of the first filter handler in the pipeline.
     * Filter handlers are named with pattern "filter-N-FilterName".
     */
    private int findFirstFilterHandler(List<String> handlerNames) {
        for (int i = 0; i < handlerNames.size(); i++) {
            if (handlerNames.get(i).startsWith("filter-")) {
                return i;
            }
        }
        return -1;
    }

}
