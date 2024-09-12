/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import javax.net.ssl.SSLException;

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
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;

import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NewKafkaProxyFrontendHandlerTest {

    public static final String SNI_HOSTNAME = "external.example.com";
    public static final String CLUSTER_HOST = "internal.example.org";
    public static final int CLUSTER_PORT = 9092;
    public static final HostPort CLUSTER_HOST_PORT = new HostPort(CLUSTER_HOST, CLUSTER_PORT);

    public static final String TLS_NEGOTIATION_ERROR = "TLS negotiation error";
    public static final HAProxyMessage HA_PROXY_MESSAGE = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
            "1.1.1.1", "2.2.2.2", 46421, 9092);
    public static final String CLIENT_SOFTWARE_NAME = "my-kafka-lib";
    public static final String CLIENT_SOFTWARE_VERSION = "1.0.0";
    private EmbeddedChannel inboundChannel;
    private ChannelHandlerContext inboundCtx;
    private EmbeddedChannel outboundChannel;
    private ChannelHandlerContext outboundCtx;

    private ChannelPromise outboundChannelTcpConnectionFuture;

    int correlectionId = 0;
    private NetFilter filter;
    private VirtualCluster virtualCluster;

    @AfterEach
    public void closeChannel() {
        if (inboundChannel != null && inboundChannel.isOpen()) {
            inboundChannel.close();
        }
        if (outboundChannel != null && outboundChannel.isOpen()) {
            outboundChannel.close();
        }
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

    private int writeSaslHandshake(KafkaAuthnHandler.SaslMechanism mechanism) {
        return writeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new SaslHandshakeRequestData()
                .setMechanism(mechanism.mechanismName()));
    }

    private int writeSaslAuthenticate(byte[] authBytes) {
        return writeRequest(SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION, new SaslAuthenticateRequestData()
                .setAuthBytes(authBytes));
    }

    private int writeInboundMetadataRequest() {
        return writeRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION, new MetadataRequestData());
    }

    private KafkaProxyFrontendHandler handler;

    private KafkaProxyFrontendHandler handler(
                                              NetFilter filter,
                                              SaslDecodePredicate dp,
                                              VirtualCluster virtualCluster) {
        return new KafkaProxyFrontendHandler(filter, dp, virtualCluster) {
            @Override
            ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap bootstrap) {
                // This is ugly... basically the EmbeddedChannel doesn't seem to handle the case
                // of a handler creating an outgoing connection and ends up
                // trying to re-register the outbound channel => IllegalStateException
                // So we override this method to short-circuit that

                outboundChannelTcpConnectionFuture = outboundChannel.newPromise();
                return outboundChannelTcpConnectionFuture.addListener(
                        future -> this.onUpstreamChannelActive(outboundChannel.pipeline().firstContext().fireChannelActive()));
            }
        };
    }

    void buildHandler(boolean saslOffloadConfigured,
                      boolean tlsConfigured,
                      Answer<Void> filterSelectServerBehaviour) {
        this.inboundChannel = new EmbeddedChannel();
        this.correlectionId = 0;

        var dp = new SaslDecodePredicate(saslOffloadConfigured);
        this.filter = mock(NetFilter.class);
        doAnswer(filterSelectServerBehaviour).when(filter).selectServer(any());
        virtualCluster = mock(VirtualCluster.class);
        final Optional<SslContext> sslContext;
        try {
            sslContext = Optional.ofNullable(tlsConfigured ? SslContextBuilder.forClient().build() : null);
        }
        catch (SSLException e) {
            throw new RuntimeException(e);
        }
        when(virtualCluster.getUpstreamSslContext()).thenReturn(sslContext);

        this.handler = handler(filter, dp, virtualCluster);
        this.inboundCtx = mock(ChannelHandlerContext.class);
        when(inboundCtx.channel()).thenReturn(inboundChannel);
        when(inboundCtx.pipeline()).thenReturn(inboundChannel.pipeline());
        when(inboundCtx.handler()).thenReturn(handler);

        this.outboundChannel = new EmbeddedChannel();
        this.outboundCtx = mock(ChannelHandlerContext.class);
        when(outboundCtx.channel()).thenReturn(outboundChannel);
        when(outboundCtx.pipeline()).thenReturn(outboundChannel.pipeline());
        when(outboundCtx.voidPromise()).thenAnswer(in -> outboundChannel.voidPromise());

    }

    // transitions from each state
    // each of the events that can happen in that state
    private void hClientConnect(KafkaProxyFrontendHandler handler) {
        final ChannelPipeline pipeline = inboundChannel.pipeline();
        if (pipeline.get(KafkaProxyFrontendHandler.class) == null) {
            pipeline.addLast(handler);
        }
        assertThat(handler.state()).isNull();
        pipeline.fireChannelActive();
        assertThat(handler.state()).isExactlyInstanceOf(ProxyChannelState.Start.class);
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
                                                                      boolean haProxy,
                                                                      boolean apiVersions) {
        return invocation -> {
            NetFilter.NetFilterContext context = invocation.getArgument(0);
            netFilterContextAssertions(context, sni, haProxy, apiVersions);
            context.initiateConnect(CLUSTER_HOST_PORT, List.of());
            context.initiateConnect(CLUSTER_HOST_PORT, List.of());
            return null;
        };
    }

    @NonNull
    private static Answer<Void> selectServerDoesNotCallInitiateConnect(
                                                                       boolean sni,
                                                                       boolean haProxy,
                                                                       boolean apiVersions) {
        return invocation -> {
            NetFilter.NetFilterContext context = invocation.getArgument(0);
            netFilterContextAssertions(context, sni, haProxy, apiVersions);
            return null;
        };
    }

    @NonNull
    private static Answer<Void> selectServerThrows(Throwable exception) {
        return invocation -> {
            throw exception;
        };
    }

    private void assertClientConnectionClosedWithNoResponse() {
        var responseAssert = assertThat(inboundChannel.<Object> readOutbound())
                .describedAs("No response sent to client")
                .asInstanceOf(InstanceOfAssertFactories.type(ByteBuf.class));
        responseAssert.extracting(ByteBuf::readableBytes).isEqualTo(0);

        assertThat(inboundChannel.isOpen())
                .describedAs("Connection to client is closed")
                .isFalse();

        assertThat(handler.state())
                .isInstanceOf(ProxyChannelState.Closed.class);
    }

    private <T extends ApiMessage> void assertClientResponse(int expectedCorrId,
                                                             @NonNull Class<T> expectedResponseBodyClass,
                                                             @Nullable Function<T, Short> errorCodeFn,
                                                             @Nullable Errors expectedErrorCode) {
        var responseAssert = assertThat(inboundChannel.<Object> readOutbound())
                .describedAs("Response sent to client")
                .asInstanceOf(InstanceOfAssertFactories.type(DecodedResponseFrame.class));
        responseAssert.extracting(DecodedResponseFrame::correlationId).isEqualTo(expectedCorrId);
        ObjectAssert<T> bodyAssert = responseAssert.extracting(DecodedResponseFrame::body)
                .describedAs("Response body is a " + expectedResponseBodyClass.getSimpleName())
                .asInstanceOf(type(expectedResponseBodyClass));
        if (errorCodeFn != null) {
            bodyAssert.describedAs("Response error code is " + expectedErrorCode)
                    .extracting(errorCodeFn).isEqualTo(expectedErrorCode.code());
        }
    }

    private void assertClientApiVersionsResponse(int corrId, Errors error) {
        assertClientResponse(
                corrId,
                ApiVersionsResponseData.class,
                ApiVersionsResponseData::errorCode,
                error);
    }

    private void assertClientSaslHandshakeResponse(int corrId, Errors error) {
        assertClientResponse(
                corrId,
                SaslHandshakeResponseData.class,
                SaslHandshakeResponseData::errorCode,
                error);
    }

    private void assertClientSaslAuthenticateResponse(int corrId, Errors error) {
        assertClientResponse(
                corrId,
                SaslAuthenticateResponseData.class,
                SaslAuthenticateResponseData::errorCode,
                error);
    }

    private void assertClientMetadataResponse(int corrId) {
        assertClientResponse(
                corrId,
                MetadataResponseData.class,
                null,
                null);
    }

    private void assertBrokerMetadataResponse(int corrId) {
        var requestAssert = assertThat(outboundChannel.<Object> readOutbound())
                .describedAs("Request sent to broker")
                .asInstanceOf(InstanceOfAssertFactories.type(DecodedRequestFrame.class));
    }

    private void assertHandlerInHaProxyState() {
        assertThat(handler.state())
                .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.HaProxy.class))
                .extracting(ProxyChannelState.HaProxy::haProxyMessage).isSameAs(HA_PROXY_MESSAGE);
    }

    private void assertHandlerInApiVersionsState(boolean haProxy) {
        var stateAssert = assertThat(handler.state())
                .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.ApiVersions.class));
        stateAssert.extracting(ProxyChannelState.ApiVersions::haProxyMessage).isEqualTo(haProxy ? HA_PROXY_MESSAGE : null);
        stateAssert.extracting(ProxyChannelState.ApiVersions::clientSoftwareName).isEqualTo(CLIENT_SOFTWARE_NAME);
        stateAssert.extracting(ProxyChannelState.ApiVersions::clientSoftwareVersion).isEqualTo(CLIENT_SOFTWARE_VERSION);
    }

    private void assertHandlerInConnectingState(
                                                boolean haProxy,
                                                List<ApiKeys> expectedBufferedRequestTypes) {
        var stateAssert = assertThat(handler.state())
                .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.Connecting.class));
        stateAssert.extracting(ProxyChannelState.Connecting::haProxyMessage).isEqualTo(haProxy ? HA_PROXY_MESSAGE : null);
        stateAssert.extracting(ProxyChannelState.Connecting::clientSoftwareName)
                .isEqualTo(expectedBufferedRequestTypes.contains(ApiKeys.API_VERSIONS) ? CLIENT_SOFTWARE_NAME : null);
        stateAssert.extracting(ProxyChannelState.Connecting::clientSoftwareVersion)
                .isEqualTo(expectedBufferedRequestTypes.contains(ApiKeys.API_VERSIONS) ? CLIENT_SOFTWARE_VERSION : null);
        stateAssert.extracting(ProxyChannelState.Connecting::bufferedMsgs, InstanceOfAssertFactories.list(DecodedRequestFrame.class))
                .map(DecodedRequestFrame::apiKey).isEqualTo(expectedBufferedRequestTypes);
    }

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

    static List<Arguments> booleanXboolean() {
        return crossProduct(bool(), bool());
    }

    static List<Arguments> booleanXbooleanXapiKey() {
        return crossProduct(bool(), bool(), apiKey());
    }

    static List<Arguments> booleanXbooleanXbooleanXapiKey() {
        return crossProduct(bool(), bool(), bool(), apiKey());
    }

    static List<Arguments> booleanXbooleanXboolean() {
        return crossProduct(bool(), bool(), bool());
    }

    @Test
    void transitionToStart() {
        // Given
        buildHandler(false, false, selectServerThrows(new AssertionError()));
        assertThat(handler.state()).isNull();

        // When
        hClientConnect(handler);

        // Then
        inboundChannel.checkException();
        assertThat(inboundChannel.config().isAutoRead()).isFalse();
        assertThat(inboundChannel.isWritable()).isTrue();

        assertThat(handler.state()).isExactlyInstanceOf(ProxyChannelState.Start.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void transitionFromStartToHaProxy(boolean sni) {
        buildHandler(false, false, selectServerThrows(new AssertionError()));
        // Given
        hClientConnect(handler);
        handler.state = new ProxyChannelState.Start(this.inboundCtx);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }

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
    void transitionFromStartToConnectingWithoutSaslOffload(
                                                           boolean sni,
                                                           boolean haProxy,
                                                           ApiKeys firstMessage) {
        // Given
        buildHandler(false, false, selectServerCallsInitiateConnect(sni, haProxy, firstMessage == ApiKeys.API_VERSIONS));

        hClientConnect(handler);
        assertThat(handler.state()).isExactlyInstanceOf(ProxyChannelState.Start.class);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }

        if (haProxy) {
            handler.state = new ProxyChannelState.HaProxy(inboundCtx, HA_PROXY_MESSAGE);
        }

        // When
        int firstCorrId = switch (firstMessage) {
            case API_VERSIONS -> writeInboundApiVersionsRequest();
            case SASL_HANDSHAKE -> writeSaslHandshake(KafkaAuthnHandler.SaslMechanism.PLAIN);
            case SASL_AUTHENTICATE -> writeSaslAuthenticate("pa55word".getBytes(StandardCharsets.UTF_8));
            case METADATA -> writeInboundMetadataRequest();
            default -> throw new IllegalArgumentException();
        };

        // Then
        inboundChannel.checkException();
        assertThat(inboundChannel.<Object> readOutbound())
                .describedAs("No response deferred until upstream connected")
                .isNull();

        assertThat(inboundChannel.config().isAutoRead()).isFalse();
        assertThat(inboundChannel.isWritable()).isTrue();

        assertHandlerInConnectingState(haProxy, List.of(firstMessage));
    }

    @ParameterizedTest
    @MethodSource("booleanXboolean")
    void transitionFromStartToConnectingWithSaslOffload(
                                                        boolean sni,
                                                        boolean haProxy) {
        Assumptions.abort();
        // Given
        buildHandler(true, false, selectServerCallsInitiateConnect(sni, haProxy, true));

        hClientConnect(handler);
        assertThat(handler.state()).isExactlyInstanceOf(ProxyChannelState.Start.class);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }

        if (haProxy) {
            handler.state = new ProxyChannelState.HaProxy(inboundCtx, HA_PROXY_MESSAGE);
        }

        int apiVersionsCorrId = writeInboundApiVersionsRequest();
        assertClientApiVersionsResponse(apiVersionsCorrId, Errors.NONE);
        assertHandlerInApiVersionsState(haProxy);

        // When
        int saslHandshakeCorrId = writeSaslHandshake(KafkaAuthnHandler.SaslMechanism.PLAIN);

        // Then
        // TODO how is this case supposed to work?
        // frontend returns the ApiVersions response and transitions to AWAITING_AUTHN
        // Then the AuthnHandler does its thing? And we end up with the AuthnEvent, which triggers SELECT_SERVER
        // Then the following request (e.g. METADATA) gets buffered??
        assertThat(true).isFalse();
        inboundChannel.checkException();
        assertThat(inboundChannel.isOpen()).isTrue();
        assertClientSaslHandshakeResponse(saslHandshakeCorrId, Errors.NONE);

        assertThat(inboundChannel.config().isAutoRead()).isFalse();
        assertThat(inboundChannel.isWritable()).isTrue();

        assertHandlerInConnectingState(haProxy, List.of(ApiKeys.API_VERSIONS));
    }

    @ParameterizedTest
    @MethodSource("booleanXboolean")
    void filterNotCallingInitiateConnectIsAnErrorWithoutSaslOffload(
                                                                    boolean sni,
                                                                    boolean haProxy) {
        // Given
        buildHandler(false, false, selectServerDoesNotCallInitiateConnect(sni, haProxy, true));

        hClientConnect(handler);
        assertThat(handler.state()).isExactlyInstanceOf(ProxyChannelState.Start.class);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }

        if (haProxy) {
            handler.state = new ProxyChannelState.HaProxy(inboundCtx, HA_PROXY_MESSAGE);
        }

        // When

        int apiVersionsCorrId = writeInboundApiVersionsRequest();

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
        buildHandler(true, false, selectServerDoesNotCallInitiateConnect(sni, haProxy, true));

        hClientConnect(handler);
        assertThat(handler.state()).isExactlyInstanceOf(ProxyChannelState.Start.class);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }

        if (haProxy) {
            handler.state = new ProxyChannelState.HaProxy(inboundCtx, HA_PROXY_MESSAGE);
        }

        int apiVersionsCorrId = writeInboundApiVersionsRequest();
        assertClientApiVersionsResponse(apiVersionsCorrId, Errors.NONE);
        assertHandlerInApiVersionsState(haProxy);

        // When
        int saslHandshakeCorrId = writeSaslHandshake(KafkaAuthnHandler.SaslMechanism.PLAIN);

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
        buildHandler(false, false, selectServerCallsInitiateConnectTwice(sni, haProxy, true));

        hClientConnect(handler);
        assertThat(handler.state()).isExactlyInstanceOf(ProxyChannelState.Start.class);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }

        if (haProxy) {
            handler.state = new ProxyChannelState.HaProxy(inboundCtx, HA_PROXY_MESSAGE);
        }

        // When
        int corrId = writeInboundApiVersionsRequest();

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
        buildHandler(true, false, selectServerCallsInitiateConnectTwice(sni, haProxy, true));

        hClientConnect(handler);
        assertThat(handler.state()).isExactlyInstanceOf(ProxyChannelState.Start.class);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }

        if (haProxy) {
            handler.state = new ProxyChannelState.HaProxy(inboundCtx, HA_PROXY_MESSAGE);
        }

        int apiVersionsCorrId = writeInboundApiVersionsRequest();
        assertClientApiVersionsResponse(apiVersionsCorrId, Errors.NONE);
        assertHandlerInApiVersionsState(haProxy);

        // When
        int saslHandshakeCorrId = writeSaslHandshake(KafkaAuthnHandler.SaslMechanism.PLAIN);

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
        buildHandler(false, false, selectServerThrows(new AssertionError()));

        hClientConnect(handler);
        assertThat(handler.state()).isExactlyInstanceOf(ProxyChannelState.Start.class);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }

        if (haProxy) {
            handler.state = new ProxyChannelState.HaProxy(inboundCtx, HA_PROXY_MESSAGE);
        }

        // When
        int corrId = writeInboundApiVersionsRequest();

        // Then
        inboundChannel.checkException();
        assertClientApiVersionsResponse(corrId, Errors.UNKNOWN_SERVER_ERROR);

        assertThat(inboundChannel.isOpen())
                .describedAs("Connection to client is closed")
                .isFalse();

        assertThat(handler.state())
                .isInstanceOf(ProxyChannelState.Closed.class);
    }

    @ParameterizedTest
    @MethodSource("booleanXboolean")
    void filterThrowingIsAnErrorWithSasl(
                                         boolean sni,
                                         boolean haProxy) {
        Assumptions.abort();
        // Given
        buildHandler(true, false, selectServerThrows(new AssertionError()));

        hClientConnect(handler);
        assertThat(handler.state()).isExactlyInstanceOf(ProxyChannelState.Start.class);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }

        if (haProxy) {
            handler.state = new ProxyChannelState.HaProxy(inboundCtx, HA_PROXY_MESSAGE);
        }

        int apiVersionsCorrId = writeInboundApiVersionsRequest();
        assertClientApiVersionsResponse(apiVersionsCorrId, Errors.NONE);
        assertThat(handler.state()).isInstanceOf(ProxyChannelState.ApiVersions.class);

        // When
        int saslHandshakeCorrId = writeSaslHandshake(KafkaAuthnHandler.SaslMechanism.PLAIN);

        // Then
        inboundChannel.checkException();
        assertClientConnectionClosedWithNoResponse();

    }

    @ParameterizedTest
    @MethodSource("booleanXbooleanXbooleanXapiKey")
    void serverChannelActivationInConnecting(
                                             boolean sni,
                                             boolean haProxy,
                                             boolean tlsConfigured,
                                             ApiKeys firstMessage) {
        // Given
        buildHandler(false, tlsConfigured, selectServerCallsInitiateConnect(sni, haProxy, firstMessage == ApiKeys.API_VERSIONS));

        hClientConnect(handler);
        if (sni) {
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }
        int metadataCorrelationId = correlectionId++;
        var metadata = decodedRequestFrame(
                MetadataRequestData.HIGHEST_SUPPORTED_VERSION,
                new MetadataRequestData(),
                metadataCorrelationId);
        handler.state = new ProxyChannelState.Connecting(inboundCtx,
                haProxy ? HA_PROXY_MESSAGE : null,
                firstMessage == ApiKeys.API_VERSIONS ? CLIENT_SOFTWARE_NAME : null,
                firstMessage == ApiKeys.API_VERSIONS ? CLIENT_SOFTWARE_VERSION : null,
                new ArrayList<>(List.of(metadata)));

        if (tlsConfigured) {
            SslHandler sslHandler = virtualCluster.getUpstreamSslContext().get().newHandler(ByteBufAllocator.DEFAULT);
            outboundChannel.pipeline().addFirst(sslHandler);
        }

        // When
        handler.onUpstreamChannelActive(outboundCtx);

        // Then
        inboundChannel.checkException();
        if (tlsConfigured) {
            var stateAssert = assertThat(handler.state).asInstanceOf(type(ProxyChannelState.NegotiatingTls.class));
            stateAssert.extracting(ProxyChannelState.NegotiatingTls::outboundCtx).isSameAs(outboundCtx);
            stateAssert.extracting(ProxyChannelState.NegotiatingTls::bufferedMsgs)
                    .asInstanceOf(InstanceOfAssertFactories.list(DecodedResponseFrame.class))
                    .isEqualTo(List.of(metadata));
        }
        else {
            var stateAssert = assertThat(handler.state).asInstanceOf(type(ProxyChannelState.Forwarding.class));
            stateAssert.extracting(ProxyChannelState.Forwarding::outboundCtx).isSameAs(outboundCtx);
            stateAssert.extracting(ProxyChannelState.Forwarding::bufferedMsgs)
                    .asInstanceOf(InstanceOfAssertFactories.list(DecodedResponseFrame.class))
                    .isEmpty();

            assertBrokerMetadataResponse(metadataCorrelationId);

            // TODO assert that _new_ messages get forwarded
        }
    }

    // TODO need assertions about buffered messages

    // TODO why don't we stop in ApiVersions any more? What are the consequences of that?

    // Buffering (and autoread is off)

}
