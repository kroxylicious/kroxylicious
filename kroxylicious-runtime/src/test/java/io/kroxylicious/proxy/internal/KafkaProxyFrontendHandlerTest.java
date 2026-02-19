/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder;
import io.kroxylicious.proxy.internal.codec.RequestDecoderTest;
import io.kroxylicious.proxy.internal.filter.TopicNameCacheFilter;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.subject.DefaultSubjectBuilder;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.model.VirtualClusterModel.VirtualClusterGatewayModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.RequestFactory;

import static io.kroxylicious.proxy.model.VirtualClusterModel.DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaProxyFrontendHandlerTest {

    public static final String SNI_HOSTNAME = "external.example.com";
    public static final String CLUSTER_HOST = "internal.example.org";
    public static final int CLUSTER_PORT = 9092;
    public static final String CLUSTER_NAME = "RandomCluster";
    EmbeddedChannel inboundChannel;
    EmbeddedChannel outboundChannel;

    int corrId = 0;
    private KafkaProxyBackendHandler backendHandler;

    ProxyChannelStateMachine proxyChannelStateMachine(EndpointBinding endpointBinding) {
        var kafkaSession = new KafkaSession(KafkaSessionState.ESTABLISHING);
        var pcsm = new ProxyChannelStateMachine(kafkaSession);
        pcsm.onBindingResolution(Objects.requireNonNull(endpointBinding), new DefaultSubjectBuilder(List.of()));
        return pcsm;
    }

    private PluginFactoryRegistry pfr;
    private FilterChainFactory fcf;

    private void writeRequest(short apiVersion, ApiMessage body) {
        int downstreamCorrelationId = corrId++;

        DecodedRequestFrame<ApiMessage> apiMessageDecodedRequestFrame = decodedRequestFrame(apiVersion, body, downstreamCorrelationId);
        inboundChannel.writeInbound(apiMessageDecodedRequestFrame);
    }

    private DecodedRequestFrame<ApiMessage> decodedRequestFrame(short apiVersion, ApiMessage body, int downstreamCorrelationId) {
        var apiKey = ApiKeys.forId(body.apiKey());

        RequestHeaderData header = new RequestHeaderData()
                .setRequestApiKey(apiKey.id)
                .setRequestApiVersion(apiVersion)
                .setClientId("client-id")
                .setCorrelationId(downstreamCorrelationId);

        return new DecodedRequestFrame<>(apiVersion, corrId, true, header, body);
    }

    @BeforeEach
    void buildChannel() {
        inboundChannel = new EmbeddedChannel();
        corrId = 0;
        this.pfr = mock(PluginFactoryRegistry.class);
        this.fcf = mock(FilterChainFactory.class);
    }

    @AfterEach
    void closeChannel() {
        inboundChannel.close();
        if (outboundChannel != null) {
            outboundChannel.close();
        }
    }

    public static List<Arguments> provideArgsForExpectedFlow() {
        var result = new ArrayList<Arguments>();
        boolean[] tf = { true, false };
        for (boolean sslConfigured : tf) {
            for (boolean haProxyConfigured : tf) {
                for (boolean sendSasl : tf) {
                    result.add(Arguments.of(sslConfigured, haProxyConfigured, sendSasl));
                }
            }
        }
        return result;
    }

    /**
     * If the client sends multiple messages immediately when connecting they may all be read
     * from the socket despite the inbound channel auto-read being disabled. We need to tolerate
     * messages being handled while in the CONNECTED/CONNECTING state before the outbound signals
     * it is active.
     */
    @Test
    void testMessageHandledAfterConnectingBeforeConnected() {
        // Given
        VirtualClusterModel virtualClusterModel = mockVirtualClusterModel(CLUSTER_NAME);
        VirtualClusterGatewayModel virtualClusterListenerModel = mock(VirtualClusterGatewayModel.class);
        when(virtualClusterListenerModel.virtualCluster()).thenReturn(virtualClusterModel);
        EndpointBinding endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.endpointGateway()).thenReturn(virtualClusterListenerModel);
        when(endpointBinding.upstreamTarget()).thenReturn(new HostPort(CLUSTER_HOST, CLUSTER_PORT));
        when(endpointBinding.nodeId()).thenReturn(null);
        var proxyChannelStateMachine = this.proxyChannelStateMachine(endpointBinding);
        KafkaProxyFrontendHandler handler = handler(new DelegatingDecodePredicate(), proxyChannelStateMachine);
        givenHandlerIsConnecting(proxyChannelStateMachine, handler, "initial");
        writeInboundApiVersionsRequest("post-connecting");

        // When
        whenConnectedAndOutboundBecomesActive(proxyChannelStateMachine);

        // Then
        assertThat(outboundClientSoftwareNames()).containsExactly("initial", "post-connecting");
    }

    @Test
    void testMessageHandledAfterConnectedBeforeOutboundActive() {
        // Given
        VirtualClusterModel virtualClusterModel = mockVirtualClusterModel("cluster");
        VirtualClusterGatewayModel virtualClusterListenerModel = mock(VirtualClusterGatewayModel.class);
        when(virtualClusterListenerModel.virtualCluster()).thenReturn(virtualClusterModel);
        EndpointBinding endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.endpointGateway()).thenReturn(virtualClusterListenerModel);
        when(endpointBinding.upstreamTarget()).thenReturn(new HostPort(CLUSTER_HOST, CLUSTER_PORT));
        when(endpointBinding.nodeId()).thenReturn(null);
        var proxyChannelStateMachine = this.proxyChannelStateMachine(endpointBinding);
        KafkaProxyFrontendHandler handler = handler(new DelegatingDecodePredicate(), proxyChannelStateMachine);
        givenHandlerIsConnected(proxyChannelStateMachine, handler);
        writeInboundApiVersionsRequest("post-connected");

        // When
        outboundChannelBecomesActive(proxyChannelStateMachine);

        // Then
        assertThat(outboundClientSoftwareNames()).containsExactly("initial", "post-connected");
    }

    @Test
    void testUnexpectedMessageReceivedBeforeConnected() {
        // Given
        VirtualClusterModel virtualClusterModel = mockVirtualClusterModel("cluster");
        VirtualClusterGatewayModel virtualClusterListenerModel = mock(VirtualClusterGatewayModel.class);
        when(virtualClusterListenerModel.virtualCluster()).thenReturn(virtualClusterModel);
        EndpointBinding endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.endpointGateway()).thenReturn(virtualClusterListenerModel);
        when(endpointBinding.upstreamTarget()).thenReturn(new HostPort(CLUSTER_HOST, CLUSTER_PORT));
        when(endpointBinding.nodeId()).thenReturn(null);
        var proxyChannelStateMachine = this.proxyChannelStateMachine(endpointBinding);
        KafkaProxyFrontendHandler handler = handler(new DelegatingDecodePredicate(), proxyChannelStateMachine);
        givenHandlerIsConnecting(proxyChannelStateMachine, handler, "initial");

        // When
        Object unexpectedMessage = new Object();
        inboundChannel.writeInbound(unexpectedMessage);

        // Then
        assertStateIsClosed(proxyChannelStateMachine);
    }

    private static VirtualClusterModel mockVirtualClusterModel(String cluster) {
        VirtualClusterModel virtualClusterModel = mock(VirtualClusterModel.class);
        when(virtualClusterModel.getClusterName()).thenReturn(cluster);
        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(CacheConfiguration.DEFAULT, cluster);
        when(virtualClusterModel.getTopicNameCacheFilter()).thenReturn(topicNameCacheFilter);
        return virtualClusterModel;
    }

    @Test
    void testHandleFrameOversizedExceptionDownstreamTlsDisabled() throws Exception {
        // Given

        VirtualClusterModel virtualClusterModel = mockVirtualClusterModel("cluster");
        VirtualClusterGatewayModel virtualClusterListenerModel = mock(VirtualClusterGatewayModel.class);
        EndpointBinding endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.endpointGateway()).thenReturn(virtualClusterListenerModel);
        when(endpointBinding.nodeId()).thenReturn(null);
        when(virtualClusterListenerModel.virtualCluster()).thenReturn(virtualClusterModel);
        when(virtualClusterListenerModel.getDownstreamSslContext()).thenReturn(Optional.empty());
        when(virtualClusterModel.gateways()).thenReturn(Map.of("default", virtualClusterListenerModel));
        var proxyChannelStateMachine = this.proxyChannelStateMachine(endpointBinding);

        KafkaProxyFrontendHandler handler = handler(new DelegatingDecodePredicate(), proxyChannelStateMachine);
        ChannelPipeline pipeline = inboundChannel.pipeline();
        pipeline.addLast(throwOnReadHandler(new DecoderException(new FrameOversizedException(5, 6))));
        pipeline.addLast(handler);

        ChannelHandlerContext mockChannelCtx = mockChannelContext();
        handler.channelActive(mockChannelCtx);

        // when
        inboundChannel.writeInbound(new Object());

        // then
        assertThat(inboundChannel.isOpen()).isFalse();
    }

    @Test
    void testHandleFrameOversizedExceptionDownstreamTlsEnabled() throws Exception {
        // Given
        VirtualClusterModel virtualClusterModel = mockVirtualClusterModel("cluster-for-oversize-frame");
        VirtualClusterGatewayModel virtualClusterListenerModel = mock(VirtualClusterGatewayModel.class);
        EndpointBinding endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.endpointGateway()).thenReturn(virtualClusterListenerModel);
        when(endpointBinding.nodeId()).thenReturn(null);
        when(virtualClusterListenerModel.virtualCluster()).thenReturn(virtualClusterModel);
        when(virtualClusterListenerModel.getDownstreamSslContext()).thenReturn(Optional.of(SslContextBuilder.forClient().build()));
        when(virtualClusterModel.gateways()).thenReturn(Map.of("default", virtualClusterListenerModel));
        var proxyChannelStateMachine = this.proxyChannelStateMachine(endpointBinding);

        KafkaProxyFrontendHandler handler = handler(new DelegatingDecodePredicate(), proxyChannelStateMachine);
        ChannelPipeline pipeline = inboundChannel.pipeline();
        pipeline.addLast(throwOnReadHandler(new DecoderException(new FrameOversizedException(5, 6))));
        pipeline.addLast(handler);

        ChannelHandlerContext mockChannelCtx = mockChannelContext();
        handler.channelActive(mockChannelCtx);

        // when
        inboundChannel.writeInbound(new Object());

        // then
        assertThat(inboundChannel.isOpen()).isFalse();
    }

    @Test
    void testUnexpectedMessageReceivedBeforeOutboundActive() {
        // Given
        VirtualClusterModel virtualClusterModel = mockVirtualClusterModel("cluster");
        VirtualClusterGatewayModel virtualClusterListenerModel = mock(VirtualClusterGatewayModel.class);
        when(virtualClusterListenerModel.virtualCluster()).thenReturn(virtualClusterModel);
        EndpointBinding endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.endpointGateway()).thenReturn(virtualClusterListenerModel);
        when(endpointBinding.upstreamTarget()).thenReturn(new HostPort(CLUSTER_HOST, CLUSTER_PORT));
        when(endpointBinding.nodeId()).thenReturn(null);
        var proxyChannelStateMachine = this.proxyChannelStateMachine(endpointBinding);

        KafkaProxyFrontendHandler handler = handler(new DelegatingDecodePredicate(), proxyChannelStateMachine);
        givenHandlerIsConnected(proxyChannelStateMachine, handler);

        // When
        Object unexpectedMessage = new Object();
        inboundChannel.writeInbound(unexpectedMessage);

        // Then
        assertStateIsClosed(proxyChannelStateMachine);
    }

    private void writeInboundApiVersionsRequest(String clientSoftwareName) {
        writeRequest(ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsRequestData()
                .setClientSoftwareName(clientSoftwareName).setClientSoftwareVersion("1.0.0"));
    }

    KafkaProxyFrontendHandler handler(DelegatingDecodePredicate dp, ProxyChannelStateMachine proxyChannelStateMachine) {
        var namedFilterDefs = List.<NamedFilterDefinition> of();
        return new KafkaProxyFrontendHandler(pfr,
                fcf,
                namedFilterDefs,
                mock(EndpointReconciler.class),
                new ApiVersionsServiceImpl(),
                dp,
                new DefaultSubjectBuilder(List.of()),
                proxyChannelStateMachine,
                Optional.empty()) {

            @Override
            Bootstrap configureBootstrap(KafkaProxyBackendHandler capturedBackendHandler, Channel inboundChannel) {
                backendHandler = capturedBackendHandler;
                outboundChannel = new EmbeddedChannel();
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
                outboundChannel.pipeline().addFirst(backendHandler);
                outboundChannel.pipeline().fireChannelRegistered();
                return outboundChannel.newPromise();
            }
        };
    }

    /**
     * Test the normal flow, in a number of configurations.
     *
     * @param sslConfigured         Whether SSL is configured
     * @param haProxyConfigured
     * @param sendSasl
     */
    @ParameterizedTest
    @MethodSource("provideArgsForExpectedFlow")
    void expectedFlow(boolean sslConfigured,
                      boolean haProxyConfigured,
                      boolean sendSasl) {

        var dp = new DelegatingDecodePredicate();

        VirtualClusterModel virtualCluster = mockVirtualClusterModel("cluster");
        var virtualClusterListenerModel = mock(VirtualClusterGatewayModel.class);
        var endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.nodeId()).thenReturn(null);
        when(endpointBinding.endpointGateway()).thenReturn(virtualClusterListenerModel);
        when(endpointBinding.upstreamTarget()).thenReturn(new HostPort(CLUSTER_HOST, CLUSTER_PORT));
        when(virtualClusterListenerModel.virtualCluster()).thenReturn(virtualCluster);
        when(virtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());
        when(virtualCluster.getClusterName()).thenReturn(CLUSTER_NAME);
        var proxyChannelStateMachine = this.proxyChannelStateMachine(endpointBinding);

        var handler = handler(dp, proxyChannelStateMachine);
        initialiseInboundChannel(proxyChannelStateMachine, handler);

        if (sslConfigured) {
            // Simulate the SSL handler
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }

        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.ClientActive.class);

        if (haProxyConfigured) {
            // Simulate the HA proxy handler
            inboundChannel.writeInbound(new HAProxyMessage(HAProxyProtocolVersion.V1,
                    HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
                    "1.2.3.4", "5.6.7.8", 65535, CLUSTER_PORT));
            assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.HaProxy.class);
        }

        if (sendSasl) {
            // Simulate the client doing SaslHandshake and SaslAuthentication,
            writeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new SaslHandshakeRequestData());
            handleConnect(proxyChannelStateMachine);
            writeRequest(SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION, new SaslAuthenticateRequestData());
        }

        // Simulate a Metadata request
        writeRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION, new MetadataRequestData());

    }

    @ParameterizedTest
    @MethodSource("requests")
    void shouldTransitionToFailedOnException(Short version, ApiMessage apiMessage) {
        // Given
        VirtualClusterModel virtualClusterModel = mockVirtualClusterModel("cluster");
        VirtualClusterGatewayModel virtualClusterListenerModel = mock(VirtualClusterGatewayModel.class);
        when(virtualClusterListenerModel.virtualCluster()).thenReturn(virtualClusterModel);
        EndpointBinding endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.endpointGateway()).thenReturn(virtualClusterListenerModel);
        when(endpointBinding.nodeId()).thenReturn(null);
        var proxyChannelStateMachine = this.proxyChannelStateMachine(endpointBinding);

        KafkaProxyFrontendHandler handler = handler(new DelegatingDecodePredicate(), proxyChannelStateMachine);
        initialiseInboundChannel(proxyChannelStateMachine, handler);
        final RequestHeaderData header = new RequestHeaderData();
        final int correlationId = 1234;
        header.setCorrelationId(correlationId);
        final ApiKeys apiKey = ApiKeys.forId(apiMessage.apiKey());
        inboundChannel.writeInbound(new DecodedRequestFrame<>(version, correlationId, true, header, apiMessage));

        // When
        inboundChannel.pipeline().fireExceptionCaught(new DecoderException("boom"));

        // Then
        assertStateIsClosed(proxyChannelStateMachine);
        assertThat(inboundChannel.<DecodedResponseFrame<?>> readOutbound()).satisfies(decodedResponseFrame -> {
            assertThat(decodedResponseFrame.apiKey()).isEqualTo(apiKey);
            assertThat(decodedResponseFrame.body()).isNotNull()
                    .isInstanceOf(apiKey.messageType.newResponse().getClass());
        });
        assertThat(inboundChannel.isOpen()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("requests")
    void shouldTransitionToFailedOnExceptionForFrameOversizedException(Short version, ApiMessage apiMessage) {
        // Given
        VirtualClusterModel model = mockVirtualClusterModel("cluster");
        when(model.getClusterName()).thenReturn(CLUSTER_NAME);
        VirtualClusterGatewayModel virtualClusterListenerModel = mock(VirtualClusterGatewayModel.class);
        when(virtualClusterListenerModel.virtualCluster()).thenReturn(model);
        EndpointBinding endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.endpointGateway()).thenReturn(virtualClusterListenerModel);
        when(endpointBinding.nodeId()).thenReturn(null);
        var proxyChannelStateMachine = this.proxyChannelStateMachine(endpointBinding);

        KafkaProxyFrontendHandler handler = handler(new DelegatingDecodePredicate(), proxyChannelStateMachine);
        initialiseInboundChannel(proxyChannelStateMachine, handler);
        final RequestHeaderData header = new RequestHeaderData();
        final int correlationId = 1234;
        header.setCorrelationId(correlationId);
        final ApiKeys apiKey = ApiKeys.forId(apiMessage.apiKey());
        inboundChannel.writeInbound(new DecodedRequestFrame<>(version, correlationId, true, header, apiMessage));

        // When
        inboundChannel.pipeline().fireExceptionCaught(new DecoderException(new FrameOversizedException(5, 6)));

        // Then
        assertStateIsClosed(proxyChannelStateMachine);
        assertThat(inboundChannel.<DecodedResponseFrame<?>> readOutbound()).satisfies(decodedResponseFrame -> {
            assertThat(decodedResponseFrame.apiKey()).isEqualTo(apiKey);
            assertThat(decodedResponseFrame.body()).isNotNull()
                    .isInstanceOf(apiKey.messageType.newResponse().getClass());
        });
        assertThat(inboundChannel.isOpen()).isFalse();
    }

    static Stream<Arguments> requests() {
        return RequestFactory.apiMessageFor(ApiKeys::latestVersion)
                .map(apiMessageVersion -> Arguments.of(apiMessageVersion.apiVersion(), apiMessageVersion.apiMessage()));
    }

    private void initialiseInboundChannel(ProxyChannelStateMachine proxyChannelStateMachine, KafkaProxyFrontendHandler handler) {
        final ChannelPipeline pipeline = inboundChannel.pipeline();
        if (pipeline.get(KafkaProxyFrontendHandler.class) == null) {
            // Add HAProxyMessageHandler before the frontend handler to intercept HAProxyMessage
            // and prevent it from reaching FilterHandlers (which only expect Kafka protocol messages)
            pipeline.addLast(new HAProxyMessageHandler(proxyChannelStateMachine.getKafkaSession(), proxyChannelStateMachine));
            pipeline.addLast(handler);
        }
        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.Startup.class);
        pipeline.fireChannelActive();
        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.ClientActive.class);
    }

    private void handleConnect(ProxyChannelStateMachine proxyChannelStateMachine) {
        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.Connecting.class);
        assertFalse(inboundChannel.config().isAutoRead(),
                "Expect inbound autoRead=true, since outbound not yet active");

        // Simulate the backend handler receiving channel active and telling the frontend handler
        outboundChannelBecomesActive(proxyChannelStateMachine);
    }

    private void outboundChannelBecomesActive(ProxyChannelStateMachine proxyChannelStateMachine) {
        outboundChannel.pipeline().fireChannelActive();
        assertTrue(inboundChannel.config().isAutoRead(),
                "Expect inbound autoRead=true, since outbound now active");
        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.Forwarding.class);
        verify(fcf).createFilters(any(FilterFactoryContext.class), any(List.class));
    }

    private List<String> outboundClientSoftwareNames() {
        return outboundApiVersionsFrames().stream()
                .map(DecodedFrame::body)
                .map(ApiVersionsRequestData::clientSoftwareName)
                .toList();
    }

    private List<DecodedRequestFrame<ApiVersionsRequestData>> outboundApiVersionsFrames() {
        ByteBuf outboundMessage;
        List<DecodedRequestFrame<ApiVersionsRequestData>> result = new ArrayList<>();
        while ((outboundMessage = outboundChannel.readOutbound()) != null) {
            assertThat(outboundMessage).isNotNull();
            ArrayList<Object> objects = new ArrayList<>();
            new KafkaRequestDecoder(RequestDecoderTest.DECODE_EVERYTHING, DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES, new ApiVersionsServiceImpl(), null)
                    .decode(outboundChannel.pipeline().firstContext(), outboundMessage, objects);
            assertThat(objects).hasSize(1);
            if (objects.get(0) instanceof DecodedRequestFrame<?> f) {
                // noinspection unchecked
                result.add((DecodedRequestFrame<ApiVersionsRequestData>) f);
            }
            else {
                throw new IllegalStateException("message was not a DecodedRequestFrame");
            }
        }
        return result;
    }

    private void whenConnectedAndOutboundBecomesActive(ProxyChannelStateMachine proxyChannelStateMachine) {
        outboundChannelBecomesActive(proxyChannelStateMachine);
        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.Forwarding.class);
    }

    private void givenHandlerIsConnected(ProxyChannelStateMachine proxyChannelStateMachine, KafkaProxyFrontendHandler handler) {
        givenHandlerIsConnecting(proxyChannelStateMachine, handler, "initial");
    }

    private void givenHandlerIsConnecting(ProxyChannelStateMachine proxyChannelStateMachine, KafkaProxyFrontendHandler handler, String initialClientSoftwareName) {
        initialiseInboundChannel(proxyChannelStateMachine, handler);
        writeInboundApiVersionsRequest(initialClientSoftwareName);
        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.Connecting.class);
    }

    private static ChannelInboundHandlerAdapter throwOnReadHandler(Exception cause) {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                ctx.fireExceptionCaught(cause);
            }
        };
    }

    // transitions from each state
    // each of the events that can happen in that state

    @Test
    void transitionsFromStart() {
        var dp = new DelegatingDecodePredicate();
        var virtualCluster = mockVirtualClusterModel("test-cluster");
        var virtualClusterListenerModel = mock(VirtualClusterGatewayModel.class);
        when(virtualClusterListenerModel.virtualCluster()).thenReturn(virtualCluster);
        when(virtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());
        EndpointBinding endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.endpointGateway()).thenReturn(virtualClusterListenerModel);
        when(endpointBinding.nodeId()).thenReturn(null);
        var proxyChannelStateMachine = proxyChannelStateMachine(endpointBinding);

        var handler = handler(dp, proxyChannelStateMachine);
        initialiseInboundChannel(proxyChannelStateMachine, handler);
        assertThat(proxyChannelStateMachine.state()).isExactlyInstanceOf(ProxyChannelState.ClientActive.class);

        // Simulate the SSL handler
        inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
    }

    private void assertStateIsClosed(ProxyChannelStateMachine proxyChannelStateMachine) {
        // As the embedded channels have their own threads we can't be certain which state we will be in here and it doesn't matter to this test
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class);
    }

    private ChannelHandlerContext mockChannelContext() {
        ChannelHandlerContext mockChannelCtx = mock(ChannelHandlerContext.class);
        ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
        doReturn(inboundChannel).when(mockChannelCtx).channel();
        doReturn(mockPipeline).when(mockChannelCtx).pipeline();
        return mockChannelCtx;
    }

}
