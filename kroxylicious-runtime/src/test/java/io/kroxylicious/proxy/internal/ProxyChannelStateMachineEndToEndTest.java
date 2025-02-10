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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Answer;

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

import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Higher level test to ensure the statemachine and the channels interact as expected
 */
class ProxyChannelStateMachineEndToEndTest {
    public static final String SNI_HOSTNAME = "external.example.com";
    public static final String CLUSTER_HOST = "internal.example.org";
    public static final int CLUSTER_PORT = 9092;
    public static final HostPort CLUSTER_HOST_PORT = new HostPort(CLUSTER_HOST, CLUSTER_PORT);

    public static final HAProxyMessage HA_PROXY_MESSAGE = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
            "1.1.1.1", "2.2.2.2", 46421, 9092);
    public static final String CLIENT_SOFTWARE_NAME = "my-kafka-lib";
    public static final String CLIENT_SOFTWARE_VERSION = "1.0.0";
    private static final Duration BACKGROUND_TASK_TIMEOUT = Duration.ofSeconds(1);
    public static final String SNAPPY_KAFKA = "SnappyKafka";
    public static final String SNAPPY_KAFKA_VERSION = "0.1.5";

    private EmbeddedChannel inboundChannel;
    private ChannelHandlerContext inboundCtx;
    private EmbeddedChannel outboundChannel;
    private final AtomicBoolean outboundClosed = new AtomicBoolean(false);

    int correlectionId = 0;
    private ProxyChannelStateMachine proxyChannelStateMachine;
    private KafkaProxyFrontendHandler handler;
    private KafkaProxyBackendHandler backendHandler;
    private boolean activateOutboundChannelAutomatically = true;

    @BeforeEach
    void setUp() {
        proxyChannelStateMachine = new ProxyChannelStateMachine();
    }

    @AfterEach
    public void closeChannel() {
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
        buildFrontendHandler(false, false, selectServerThrows(new AssertionError()));
        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.Startup.class);

        // When
        hClientConnect(handler);

        // Then
        inboundChannel.checkException();
        assertThat(inboundChannel.config().isAutoRead()).isFalse();
        assertThat(inboundChannel.isWritable()).isTrue();

        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.ClientActive.class);
    }

    @ParameterizedTest
    @MethodSource("clientException")
    void toClientActiveThenException(Throwable clientException) {
        // Given
        buildHandlerInClientActiveState(false, selectServerThrows(new AssertionError()), false);

        // When
        handler.exceptionCaught(inboundCtx, clientException);

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse();
    }

    @Test
    void toClientActiveThenUnexpectedMessage() {
        // Given
        buildHandlerInClientActiveState(false, selectServerThrows(new AssertionError()), false);

        // When
        inboundChannel.writeInbound("unexpected");

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse();
    }

    @Test
    void toClientActiveThenInactive() {
        // Given
        buildHandlerInClientActiveState(false, selectServerThrows(new AssertionError()), false);

        // When
        inboundChannel.close();

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse();
    }

    @ParameterizedTest
    @MethodSource("bool")
    void clientActiveToHaProxy(boolean sni) {
        buildHandlerInClientActiveState(false, selectServerThrows(new AssertionError()), sni);

        // When
        inboundChannel.writeInbound(HA_PROXY_MESSAGE);

        // Then
        inboundChannel.checkException();
        assertThat(inboundChannel.config().isAutoRead()).isFalse();
        assertThat(inboundChannel.isWritable()).isTrue();

        assertHandlerInHaProxyState();

    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKey")
    void clientActiveToConnectingWithoutSaslOffload(
                                                    boolean sni,
                                                    boolean haProxy,
                                                    ApiKeys firstMessage) {
        // Given
        // Keeps the statemachine from automatically progressing so we can assert intermediate state
        activateOutboundChannelAutomatically = false;
        buildHandlerInClientActiveState(false, selectServerCallsInitiateConnect(sni, haProxy, firstMessage == ApiKeys.API_VERSIONS), sni);

        if (haProxy) {
            proxyChannelStateMachine.forceState(
                    new ProxyChannelState.HaProxy(HA_PROXY_MESSAGE),
                    handler,
                    backendHandler);
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

        assertThat(inboundChannel.config().isAutoRead()).isFalse();
        assertThat(inboundChannel.isWritable()).isTrue();

        assertHandlerInConnectingState(haProxy, List.of(firstMessage));
    }

    private void buildHandlerInClientActiveState(
                                                 boolean saslOffloadConfigured,
                                                 Answer<Void> filterSelectServerBehaviour, boolean sni) {
        buildFrontendHandler(saslOffloadConfigured, false, filterSelectServerBehaviour);

        hClientConnect(handler);
        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.ClientActive.class);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }
    }

    @ParameterizedTest
    @MethodSource("booleanXboolean")
    @Disabled("While we figure out authentication offload")
    void clientActiveToConnectingWithSaslOffload(
                                                 boolean sni,
                                                 boolean haProxy) {
        // Given
        buildHandlerInClientActiveState(true, selectServerCallsInitiateConnect(sni, haProxy, true), sni);

        if (haProxy) {
            proxyChannelStateMachine.forceState(
                    new ProxyChannelState.HaProxy(HA_PROXY_MESSAGE),
                    handler,
                    backendHandler);
        }

        int apiVersionsCorrId = writeInboundApiVersionsRequest();
        assertNextClientResponseIsApiVersionsError(apiVersionsCorrId, Errors.NONE);
        assertHandlerInApiVersionsState(haProxy);

        // When
        int saslHandshakeCorrId = writeSaslPlainHandshake();

        // Then
        // TODO how is this case supposed to work?
        // frontend returns the ApiVersions response and transitions to AWAITING_AUTHN
        // Then the AuthnHandler does its thing? And we end up with the AuthnEvent, which triggers SELECT_SERVER
        // Then the following request (e.g. METADATA) gets buffered??
        assertThat(true).isFalse();
        inboundChannel.checkException();
        assertThat(inboundChannel.isOpen()).isTrue();
        assertClientResponse(
                saslHandshakeCorrId,
                SaslHandshakeResponseData.class,
                SaslHandshakeResponseData::errorCode,
                Errors.NONE);

        assertThat(inboundChannel.config().isAutoRead()).isFalse();
        assertThat(inboundChannel.isWritable()).isTrue();

        assertHandlerInConnectingState(haProxy, List.of(ApiKeys.API_VERSIONS));
    }

    @ParameterizedTest
    @MethodSource("booleanXboolean")
    void filterNotCallingInitiateConnectIsAnErrorWithoutSaslOffload(boolean sni,
                                                                    boolean haProxy) {
        // Given
        buildHandlerInClientActiveState(false, selectServerDoesNotCallInitiateConnect(sni, haProxy), sni);

        if (haProxy) {
            proxyChannelStateMachine.forceState(
                    new ProxyChannelState.HaProxy(HA_PROXY_MESSAGE),
                    handler,
                    backendHandler);
        }

        // When

        writeInboundApiVersionsRequest();

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse();
    }

    @ParameterizedTest
    @MethodSource("booleanXboolean")
    void filterNotCallingInitiateConnectIsAnErrorWithSaslOffload(
                                                                 boolean sni,
                                                                 boolean haProxy) {
        // Given
        buildHandlerInClientActiveState(true, selectServerDoesNotCallInitiateConnect(sni, haProxy), sni);

        if (haProxy) {
            proxyChannelStateMachine.forceState(
                    new ProxyChannelState.HaProxy(HA_PROXY_MESSAGE),
                    handler,
                    backendHandler);
        }

        int apiVersionsCorrId = writeInboundApiVersionsRequest();
        assertNextClientResponseIsApiVersionsError(apiVersionsCorrId, Errors.NONE);
        assertHandlerInApiVersionsState(haProxy);

        // When
        writeSaslPlainHandshake();

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse();
    }

    @ParameterizedTest
    @MethodSource("booleanXboolean")
    void filterCallingInitiateConnectTwiceIsAnErrorWithoutSaslOffload(
                                                                      boolean sni,
                                                                      boolean haProxy) {
        // Given
        buildHandlerInClientActiveState(false, selectServerCallsInitiateConnectTwice(sni, haProxy), sni);

        if (haProxy) {
            proxyChannelStateMachine.forceState(
                    new ProxyChannelState.HaProxy(HA_PROXY_MESSAGE),
                    handler,
                    backendHandler);
        }

        // When
        writeInboundApiVersionsRequest();

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse();
    }

    @ParameterizedTest
    @MethodSource("booleanXboolean")
    void filterCallingInitiateConnectTwiceIsAnErrorWithSaslOffload(
                                                                   boolean sni,
                                                                   boolean haProxy) {
        // Given
        buildHandlerInClientActiveState(true, selectServerCallsInitiateConnectTwice(sni, haProxy), sni);

        if (haProxy) {
            proxyChannelStateMachine.forceState(
                    new ProxyChannelState.HaProxy(HA_PROXY_MESSAGE),
                    handler,
                    backendHandler);
        }

        int apiVersionsCorrId = writeInboundApiVersionsRequest();
        assertNextClientResponseIsApiVersionsError(apiVersionsCorrId, Errors.NONE);
        assertHandlerInApiVersionsState(haProxy);

        // When
        writeSaslPlainHandshake();

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse();
    }

    @ParameterizedTest
    @MethodSource("booleanXboolean")
    void filterThrowingIsAnErrorWithoutSaslOffload(
                                                   boolean sni,
                                                   boolean haProxy) {
        // Given
        buildHandlerInClientActiveState(false, selectServerThrows(new AssertionError()), sni);

        if (haProxy) {
            proxyChannelStateMachine.forceState(
                    new ProxyChannelState.HaProxy(HA_PROXY_MESSAGE),
                    handler,
                    backendHandler);
        }

        // When
        int corrId = writeInboundApiVersionsRequest();

        // Then
        inboundChannel.checkException();
        assertNextClientResponseIsApiVersionsError(corrId, Errors.UNKNOWN_SERVER_ERROR);

        assertEverythingClosed();
    }

    @ParameterizedTest
    @MethodSource("booleanXboolean")
    @Disabled("While we figure out authentication offload")
    void filterThrowingIsAnErrorWithSaslOffload(
                                                boolean sni,
                                                boolean haProxy) {
        // Given
        buildHandlerInClientActiveState(true, selectServerThrows(new AssertionError()), sni);

        if (haProxy) {
            proxyChannelStateMachine.forceState(
                    new ProxyChannelState.HaProxy(HA_PROXY_MESSAGE),
                    handler,
                    backendHandler);
        }

        int apiVersionsCorrId = writeInboundApiVersionsRequest();
        assertNextClientResponseIsApiVersionsError(apiVersionsCorrId, Errors.NONE);
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.ApiVersions.class);

        // When
        writeSaslPlainHandshake();

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse();

    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKey")
    void plainServerChannelActivationInConnecting(
                                                  boolean sni,
                                                  boolean haProxy,
                                                  ApiKeys firstMessage) {
        // Given
        activateOutboundChannelAutomatically = false;
        buildHandlerInConnectingState(sni, haProxy, false, firstMessage);

        // When
        outboundChannel.pipeline().fireChannelActive();

        // Then
        inboundChannel.checkException();
        outboundChannel.checkException();
        assertThat(outboundChannel.<Object> readOutbound())
                .describedAs("Buffered request should be forwarded to server")
                .isNotNull();

        assertProxyActive();
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
        activateOutboundChannelAutomatically = false;
        final DecodedRequestFrame<ApiMessage> requestFrame = buildHandlerInConnectingState(sni, haProxy, false, firstMessage);

        // When
        outboundChannel.pipeline().fireExceptionCaught(serverException);

        // Then
        inboundChannel.checkException();
        outboundChannel.checkException();

        assertNextClientResponseIsErrorFor(requestFrame);
        assertNoMoreResponses();
        assertEverythingClosed();
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
        buildHandlerInConnectingState(sni, haProxy, false, firstMessage);

        // When
        outboundChannel.pipeline().fireChannelInactive();

        // Then
        inboundChannel.checkException();
        outboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse();
    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKey")
    void tlsServerChannelActivationInConnecting(
                                                boolean sni,
                                                boolean haProxy,
                                                ApiKeys firstMessage) {
        // Given
        var firstRequest = buildHandlerInConnectingState(sni, haProxy, true, firstMessage);

        // When
        outboundChannel.pipeline().fireChannelActive();

        // Then
        inboundChannel.checkException();

        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Connecting.class);

        assertThat(handler.bufferedMsgs)
                .asInstanceOf(InstanceOfAssertFactories.list(DecodedResponseFrame.class))
                .isEqualTo(List.of(firstRequest));

        assertThat(inboundChannel.config().isAutoRead())
                .describedAs("Client autoread should be off while connecting to server")
                .isFalse();
    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKey")
    void tlsHandshakeFail(
                          boolean sni,
                          boolean haProxy,
                          ApiKeys firstMessage) {
        // Given
        final DecodedRequestFrame<ApiMessage> requestFrame = buildHandlerInConnectingState(sni, haProxy, true, firstMessage);
        outboundChannel.pipeline().fireChannelActive();

        // When
        outboundChannel.pipeline().fireUserEventTriggered(new SslHandshakeCompletionEvent(new SSLHandshakeException("Oops")));

        // Then
        inboundChannel.checkException();
        outboundChannel.checkException();

        assertNextClientResponseIsErrorFor(requestFrame);

        assertThat(inboundChannel.config().isAutoRead())
                .describedAs("Client autoread should be off while connecting to server")
                .isFalse();

        assertEverythingClosed();
    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXapiKey")
    void tlsHandshakeSuccess(
                             boolean sni,
                             boolean haProxy,
                             ApiKeys firstMessage) {
        // Given
        buildHandlerInConnectingState(sni, haProxy, true, firstMessage);
        outboundChannel.pipeline().fireChannelActive();

        // When
        outboundChannel.pipeline().fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS);

        // Then
        inboundChannel.checkException();

        assertProxyActive();
    }

    private void assertProxyActive() {
        assertThat(proxyChannelStateMachine.state())
                .isInstanceOf(ProxyChannelState.Forwarding.class);

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

    @NonNull
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
        int downstreamCorrelationId = correlectionId++;

        DecodedRequestFrame<ApiMessage> apiMessageDecodedRequestFrame = decodedRequestFrame(apiVersion, body, downstreamCorrelationId);
        inboundChannel.writeInbound(apiMessageDecodedRequestFrame);
        return downstreamCorrelationId;
    }

    private int writeInboundApiVersionsRequest() {
        return writeRequest(ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsRequestData()
                .setClientSoftwareName(CLIENT_SOFTWARE_NAME)
                .setClientSoftwareVersion(CLIENT_SOFTWARE_VERSION));
    }

    private int writeSaslPlainHandshake() {
        return writeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new SaslHandshakeRequestData()
                .setMechanism(KafkaAuthnHandler.SaslMechanism.PLAIN.mechanismName()));
    }

    private void writeSaslAuthenticate(byte[] authBytes) {
        writeRequest(SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION, new SaslAuthenticateRequestData()
                .setAuthBytes(authBytes));
    }

    private void writeInboundMetadataRequest() {
        writeRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION, new MetadataRequestData());
    }

    private KafkaProxyFrontendHandler handler(
                                              NetFilter filter,
                                              SaslDecodePredicate dp,
                                              VirtualClusterModel virtualClusterModel) {
        return new KafkaProxyFrontendHandler(filter, dp, virtualClusterModel, proxyChannelStateMachine) {
            @NonNull
            @Override
            Bootstrap configureBootstrap(KafkaProxyBackendHandler capturedBackendHandler, Channel inboundChannel) {
                ProxyChannelStateMachineEndToEndTest.this.backendHandler = capturedBackendHandler;
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
            ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap bootstrap) {
                // This is ugly... basically the EmbeddedChannel doesn't seem to handle the case
                // of a handler creating an outgoing connection and ends up
                // trying to re-register the outbound channel => IllegalStateException
                // So we override this method to short-circuit that
                outboundChannel.pipeline().addFirst(ProxyChannelStateMachineEndToEndTest.this.backendHandler);
                outboundChannel.pipeline().fireChannelRegistered();
                if (ProxyChannelStateMachineEndToEndTest.this.activateOutboundChannelAutomatically) {
                    outboundChannel.pipeline().fireChannelActive();
                }
                return outboundChannel.newPromise();
            }
        };
    }

    void buildFrontendHandler(boolean saslOffloadConfigured,
                              boolean tlsConfigured,
                              Answer<Void> filterSelectServerBehaviour) {
        this.inboundChannel = new EmbeddedChannel();
        this.correlectionId = 0;

        var dp = new SaslDecodePredicate(saslOffloadConfigured);
        NetFilter filter = mock(NetFilter.class);
        doAnswer(filterSelectServerBehaviour).when(filter).selectServer(any());
        VirtualClusterModel virtualClusterModel = mock(VirtualClusterModel.class);
        final Optional<SslContext> sslContext;
        try {
            sslContext = Optional.ofNullable(tlsConfigured ? SslContextBuilder.forClient().build() : null);
        }
        catch (SSLException e) {
            throw new RuntimeException(e);
        }
        when(virtualClusterModel.getUpstreamSslContext()).thenReturn(sslContext);

        this.handler = handler(filter, dp, virtualClusterModel);
        this.inboundCtx = mock(ChannelHandlerContext.class);
        when(inboundCtx.channel()).thenReturn(inboundChannel);
        when(inboundCtx.pipeline()).thenReturn(inboundChannel.pipeline());
        when(inboundCtx.handler()).thenReturn(handler);
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
    private void hClientConnect(KafkaProxyFrontendHandler handler) {
        final ChannelPipeline pipeline = inboundChannel.pipeline();
        if (pipeline.get(KafkaProxyFrontendHandler.class) == null) {
            pipeline.addLast(handler);
        }
        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.Startup.class);
        pipeline.fireChannelActive();
        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.ClientActive.class);
    }

    private static void netFilterContextAssertions(
                                                   NetFilter.NetFilterContext context,
                                                   boolean sni,
                                                   boolean haProxy,
                                                   boolean apiVersions) {
        assertThat(context.sniHostname()).isEqualTo(sni ? SNI_HOSTNAME : null);
        assertThat(context.authorizedId()).isNull();
        assertThat(context.clientHost()).isEqualTo(haProxy ? HA_PROXY_MESSAGE.sourceAddress() : "embedded"); // hard-coded for EmbeddedChannel
        assertThat(context.clientPort()).isEqualTo(haProxy ? HA_PROXY_MESSAGE.sourcePort() : -1); // hard-coded for EmbeddedChannel
        assertThat(context.clientSoftwareName()).isEqualTo(apiVersions ? CLIENT_SOFTWARE_NAME : null);
        assertThat(context.clientSoftwareVersion()).isEqualTo(apiVersions ? CLIENT_SOFTWARE_VERSION : null);
        assertThat(context.localAddress()).hasToString("embedded"); // hard-coded for EmbeddedChannel
        assertThat(context.srcAddress()).hasToString("embedded"); // hard-coded for EmbeddedChannel
    }

    @NonNull
    private static Answer<Void> selectServerCallsInitiateConnect(boolean sni,
                                                                 boolean haProxy,
                                                                 boolean apiVersions) {
        return invocation -> {
            NetFilter.NetFilterContext context = invocation.getArgument(0);
            netFilterContextAssertions(context, sni, haProxy, apiVersions);
            context.initiateConnect(CLUSTER_HOST_PORT, List.of());
            return null;
        };
    }

    @NonNull
    private static Answer<Void> selectServerCallsInitiateConnectTwice(
                                                                      boolean sni,
                                                                      boolean haProxy) {
        return invocation -> {
            NetFilter.NetFilterContext context = invocation.getArgument(0);
            netFilterContextAssertions(context, sni, haProxy, true);
            context.initiateConnect(CLUSTER_HOST_PORT, List.of());
            context.initiateConnect(CLUSTER_HOST_PORT, List.of());
            return null;
        };
    }

    @NonNull
    private static Answer<Void> selectServerDoesNotCallInitiateConnect(
                                                                       boolean sni,
                                                                       boolean haProxy) {
        return invocation -> {
            NetFilter.NetFilterContext context = invocation.getArgument(0);
            netFilterContextAssertions(context, sni, haProxy, true);
            return null;
        };
    }

    @NonNull
    private static Answer<Void> selectServerThrows(Throwable exception) {
        return invocation -> {
            throw exception;
        };
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

    private void assertClientConnectionClosedWithNoResponse() {
        assertNoClientResponses();

        assertThat(inboundChannel.isOpen())
                .describedAs("Connection to client is closed")
                .isFalse();

        assertEverythingClosed();
    }

    private <T extends ApiMessage> void assertClientResponse(int expectedCorrId,
                                                             @NonNull Class<T> expectedResponseBodyClass,
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

    private void assertNextClientResponseIsApiVersionsError(int corrId, Errors error) {
        assertClientResponse(
                corrId,
                ApiVersionsResponseData.class,
                ApiVersionsResponseData::errorCode,
                error);
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

    private void assertHandlerInHaProxyState() {
        assertThat(proxyChannelStateMachine.state())
                .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.HaProxy.class))
                .extracting(ProxyChannelState.HaProxy::haProxyMessage).isSameAs(HA_PROXY_MESSAGE);
    }

    private void assertHandlerInApiVersionsState(boolean haProxy) {
        var stateAssert = assertThat(proxyChannelStateMachine.state())
                .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.ApiVersions.class));
        stateAssert.extracting(ProxyChannelState.ApiVersions::haProxyMessage).isEqualTo(haProxy ? HA_PROXY_MESSAGE : null);
        stateAssert.extracting(ProxyChannelState.ApiVersions::clientSoftwareName).isEqualTo(CLIENT_SOFTWARE_NAME);
        stateAssert.extracting(ProxyChannelState.ApiVersions::clientSoftwareVersion).isEqualTo(CLIENT_SOFTWARE_VERSION);
    }

    private void assertHandlerInConnectingState(
                                                boolean haProxy,
                                                List<ApiKeys> expectedBufferedRequestTypes) {
        var stateAssert = assertThat(proxyChannelStateMachine.state())
                .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.Connecting.class));
        stateAssert.extracting(ProxyChannelState.Connecting::haProxyMessage).isEqualTo(haProxy ? HA_PROXY_MESSAGE : null);
        stateAssert.extracting(ProxyChannelState.Connecting::clientSoftwareName)
                .isEqualTo(expectedBufferedRequestTypes.contains(ApiKeys.API_VERSIONS) ? CLIENT_SOFTWARE_NAME : null);
        stateAssert.extracting(ProxyChannelState.Connecting::clientSoftwareVersion)
                .isEqualTo(expectedBufferedRequestTypes.contains(ApiKeys.API_VERSIONS) ? CLIENT_SOFTWARE_VERSION : null);
        assertThat(handler.bufferedMsgs).asInstanceOf(InstanceOfAssertFactories.list(DecodedRequestFrame.class))
                .map(DecodedRequestFrame::apiKey).isEqualTo(expectedBufferedRequestTypes);
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

    static List<Arguments> booleanXboolean() {
        return crossProduct(bool(), bool());
    }

    static List<Arguments> booleanXbooleanXapiKey() {
        return crossProduct(bool(), bool(), apiKey());
    }

    static List<Arguments> booleanXbooleanXapiKeyXserverException() {
        return crossProduct(bool(), bool(), apiKey(), serverException());
    }

    private DecodedRequestFrame<ApiMessage> buildHandlerInConnectingState(
                                                                          boolean sni,
                                                                          boolean haProxy,
                                                                          boolean tlsConfigured,
                                                                          ApiKeys firstMessage) {
        buildFrontendHandler(false, tlsConfigured, selectServerCallsInitiateConnect(sni, haProxy, firstMessage == ApiKeys.API_VERSIONS));

        hClientConnect(handler);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }
        proxyChannelStateMachine.forceState(
                new ProxyChannelState.SelectingServer(
                        haProxy ? HA_PROXY_MESSAGE : null,
                        firstMessage == ApiKeys.API_VERSIONS ? CLIENT_SOFTWARE_NAME : null,
                        firstMessage == ApiKeys.API_VERSIONS ? CLIENT_SOFTWARE_VERSION : null),
                handler,
                backendHandler);

        inboundChannel.config().setAutoRead(false);

        final DecodedRequestFrame<ApiMessage> firstRequest = apiKeyToMessage(firstMessage);
        handler.bufferMsg(firstRequest);

        handler.inSelectingServer();

        return firstRequest;
    }

    // TODO backpressure

    @NonNull
    private DecodedRequestFrame<ApiMessage> apiKeyToMessage(ApiKeys firstMessage) {
        return switch (firstMessage) {
            case API_VERSIONS -> decodedRequestFrame(ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsRequestData()
                    .setClientSoftwareName(CLIENT_SOFTWARE_NAME)
                    .setClientSoftwareVersion(CLIENT_SOFTWARE_VERSION), correlectionId++);
            case SASL_HANDSHAKE -> decodedRequestFrame(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new SaslHandshakeRequestData()
                    .setMechanism(KafkaAuthnHandler.SaslMechanism.PLAIN.mechanismName()), correlectionId++);
            case SASL_AUTHENTICATE -> decodedRequestFrame(SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION, new SaslAuthenticateRequestData()
                    .setAuthBytes("pa55word".getBytes(StandardCharsets.UTF_8)), correlectionId++);
            case METADATA -> decodedRequestFrame(MetadataRequestData.HIGHEST_SUPPORTED_VERSION, new MetadataRequestData(), correlectionId++);
            default -> throw new IllegalArgumentException();
        };
    }

    private void assertNextClientResponseIsErrorFor(DecodedRequestFrame<ApiMessage> requestFrame) {
        switch (requestFrame.apiKey()) {
            case API_VERSIONS -> assertNextClientResponseIsApiVersionsError(requestFrame.correlationId(), Errors.UNKNOWN_SERVER_ERROR);
            case SASL_HANDSHAKE -> assertNextClientResponseIsSaslHandshakeError(requestFrame.correlationId());
            case SASL_AUTHENTICATE -> assertNextClientResponseIsSaslAuthenticateError(requestFrame.correlationId());
            case METADATA -> assertNextClientResponseIsMetadataError(requestFrame.correlationId());
            default -> throw new UnsupportedOperationException("unsupported apiKey: " + requestFrame.apiKey());
        }
    }

    private void assertEverythingClosed() {
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class);
        if (outboundChannel != null) {
            await("outboundClosed").atMost(BACKGROUND_TASK_TIMEOUT).untilTrue(outboundClosed);
            assertThat(outboundChannel.isActive()).isFalse();
            assertThat(outboundChannel.isOpen()).isFalse();
        }
        assertThat(inboundChannel.isActive()).isFalse();
        assertThat(inboundChannel.isOpen()).isFalse();

        await("transition to closed").atMost(BACKGROUND_TASK_TIMEOUT)
                .untilAsserted(() -> assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class));
    }

}
