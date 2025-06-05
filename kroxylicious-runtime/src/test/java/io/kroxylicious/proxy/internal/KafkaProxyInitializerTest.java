/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SniHandler;
import io.netty.util.internal.StringUtil;

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.ServiceBasedPluginFactoryRegistry;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.internal.filter.ApiVersionsDowngradeFilter;
import io.kroxylicious.proxy.internal.filter.ApiVersionsIntersectFilter;
import io.kroxylicious.proxy.internal.net.Endpoint;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointBindingResolver;
import io.kroxylicious.proxy.internal.net.EndpointResolutionException;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.proxy.internal.KafkaProxyInitializer.LOGGING_INBOUND_ERROR_HANDLER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProxyInitializerTest {

    @Mock(strictness = Mock.Strictness.LENIENT)
    private SocketChannel channel;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ChannelPipeline channelPipeline;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private EndpointBinding vcb;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private EventLoop eventLoop;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ServerSocketChannel serverSocketChannel;

    @Captor
    ArgumentCaptor<ChannelInboundHandlerAdapter> plainChannelResolverCaptor;

    private ServiceBasedPluginFactoryRegistry pfr;
    private KafkaProxyInitializer kafkaProxyInitializer;
    private CompletionStage<EndpointBinding> bindingStage;
    private VirtualClusterModel virtualClusterModel;
    private FilterChainFactory filterChainFactory;

    @BeforeEach
    void setUp() {
        virtualClusterModel = buildVirtualCluster(false, false);
        pfr = new ServiceBasedPluginFactoryRegistry();
        bindingStage = CompletableFuture.completedStage(vcb);
        filterChainFactory = new FilterChainFactory(pfr, List.of());
        final InetSocketAddress localhost = new InetSocketAddress(0);
        when(channel.pipeline()).thenReturn(channelPipeline);
        when(channel.parent()).thenReturn(serverSocketChannel);
        when(channel.eventLoop()).thenReturn(eventLoop);
        when(channel.localAddress()).thenReturn(InetSocketAddress.createUnresolved("localhost", 9099));

        when(serverSocketChannel.localAddress()).thenReturn(localhost);
        when(vcb.endpointGateway()).thenReturn(virtualClusterModel.gateways().values().iterator().next());
    }

    private VirtualClusterModel buildVirtualCluster(boolean logNetwork, boolean logFrames) {
        final Optional<Tls> tls = Optional.empty();
        VirtualClusterModel testCluster = new VirtualClusterModel("testCluster", new TargetCluster("localhost:9090", tls), logNetwork,
                logFrames, List.of());
        testCluster.addGateway("defaullt", mock(ClusterNetworkAddressConfigProvider.class), tls);
        return testCluster;

    }

    @Test
    void shouldInitialisePlainChannel() {
        // Given
        kafkaProxyInitializer = createKafkaProxyInitializer(false, (endpoint, sniHostname) -> bindingStage, Map.of());
        // When
        kafkaProxyInitializer.initChannel(channel);

        // Then
        verify(channelPipeline).addLast(eq("plainResolver"), isA(ChannelInboundHandlerAdapter.class));
        assertErrorHandlerAdded();
        verify(channelPipeline, times(0)).addLast(anyString(), isA(SniHandler.class));
    }

    @Test
    void shouldResolveWhenPlainChannelActivated() throws Exception {
        // Given
        final EndpointBindingResolver bindingResolver = mock(EndpointBindingResolver.class);
        when(bindingResolver.resolve(any(Endpoint.class), isNull())).thenReturn(bindingStage);
        kafkaProxyInitializer = createKafkaProxyInitializer(false, bindingResolver, Map.of());
        when(channelPipeline.addLast(eq("plainResolver"), plainChannelResolverCaptor.capture())).thenReturn(channelPipeline);

        kafkaProxyInitializer.initChannel(channel);
        final ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);

        // When
        plainChannelResolverCaptor.getValue().channelActive(channelHandlerContext);

        // Then
        verify(bindingResolver).resolve(any(Endpoint.class), isNull());
    }

    @Test
    void shouldRemovePlainChannelInitializerOnceComplete() throws Exception {
        // Given
        final EndpointBindingResolver bindingResolver = mock(EndpointBindingResolver.class);
        when(bindingResolver.resolve(any(Endpoint.class), isNull())).thenReturn(bindingStage);
        kafkaProxyInitializer = createKafkaProxyInitializer(false, bindingResolver, Map.of());
        when(channelPipeline.addLast(eq("plainResolver"), plainChannelResolverCaptor.capture())).thenReturn(channelPipeline);

        kafkaProxyInitializer.initChannel(channel);
        final ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);

        // When
        plainChannelResolverCaptor.getValue().channelActive(channelHandlerContext);

        // Then
        verify(channelPipeline).remove(plainChannelResolverCaptor.getValue());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldAddCommonHandlersOnBindingComplete(boolean tls) {
        // Given
        when(vcb.endpointGateway())
                .thenReturn(virtualClusterModel.gateways().values().iterator().next());
        kafkaProxyInitializer = createKafkaProxyInitializer(tls, (endpoint, sniHostname) -> bindingStage, Map.of());

        // When
        kafkaProxyInitializer.addHandlers(channel, vcb);

        // Then
        final InOrder orderedVerifyer = inOrder(channelPipeline);
        verifyErrorHandlerRemoved(orderedVerifyer);
        verifyEncoderAndOrdererAdded(orderedVerifyer);
        verifyFrontendHandlerAdded(orderedVerifyer);
        verifyErrorHandlerAdded(orderedVerifyer);
        Mockito.verifyNoMoreInteractions(channelPipeline);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldAddFrameLoggerOnBindingComplete(boolean tls) {
        // Given
        virtualClusterModel = buildVirtualCluster(false, true);
        when(vcb.endpointGateway()).thenReturn(virtualClusterModel.gateways().values().iterator().next());
        kafkaProxyInitializer = createKafkaProxyInitializer(tls, (endpoint, sniHostname) -> bindingStage, Map.of());

        // When
        kafkaProxyInitializer.addHandlers(channel, vcb);

        // Then
        final InOrder orderedVerifyer = inOrder(channelPipeline);
        verifyErrorHandlerRemoved(orderedVerifyer);
        verifyEncoderAndOrdererAdded(orderedVerifyer);
        verifyFrameLoggerAdded(orderedVerifyer);
        verifyFrontendHandlerAdded(orderedVerifyer);
        verifyErrorHandlerAdded(orderedVerifyer);
        Mockito.verifyNoMoreInteractions(channelPipeline);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldAddNetworkLoggerOnBindingComplete(boolean tls) {
        // Given
        virtualClusterModel = buildVirtualCluster(true, false);
        when(vcb.endpointGateway()).thenReturn(virtualClusterModel.gateways().values().iterator().next());
        kafkaProxyInitializer = createKafkaProxyInitializer(tls, (endpoint, sniHostname) -> bindingStage, Map.of());

        // When
        kafkaProxyInitializer.addHandlers(channel, vcb);

        // Then
        final InOrder orderedVerifyer = inOrder(channelPipeline);
        verifyErrorHandlerRemoved(orderedVerifyer);
        verifyNetworkLoggerAdded(orderedVerifyer);
        verifyEncoderAndOrdererAdded(orderedVerifyer);
        verifyFrontendHandlerAdded(orderedVerifyer);
        verifyErrorHandlerAdded(orderedVerifyer);
        Mockito.verifyNoMoreInteractions(channelPipeline);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldAddAuthnHandlersOnBindingComplete(boolean tls) {
        // Given
        virtualClusterModel = buildVirtualCluster(false, false);
        when(vcb.endpointGateway()).thenReturn(virtualClusterModel.gateways().values().iterator().next());
        final AuthenticateCallbackHandler plainHandler = mock(AuthenticateCallbackHandler.class);
        kafkaProxyInitializer = createKafkaProxyInitializer(tls, (endpoint, sniHostname) -> bindingStage, Map.of(KafkaAuthnHandler.SaslMechanism.PLAIN, plainHandler));

        // When
        kafkaProxyInitializer.addHandlers(channel, vcb);

        // Then
        final InOrder orderedVerifier = inOrder(channelPipeline);
        verifyErrorHandlerRemoved(orderedVerifier);
        verifyEncoderAndOrdererAdded(orderedVerifier);
        orderedVerifier.verify(channelPipeline).addLast(any(KafkaAuthnHandler.class));
        verifyFrontendHandlerAdded(orderedVerifier);
        verifyErrorHandlerAdded(orderedVerifier);
        Mockito.verifyNoMoreInteractions(channelPipeline);
    }

    @Test
    void shouldCreateFilters() {
        // Given
        final FilterChainFactory fcf = mock(FilterChainFactory.class);
        when(vcb.upstreamTarget()).thenReturn(new HostPort("upstream.broker.kafka", 9090));
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        final KafkaProxyInitializer.InitalizerNetFilter initalizerNetFilter = new KafkaProxyInitializer.InitalizerNetFilter(mock(SaslDecodePredicate.class),
                channel,
                vcb,
                pfr,
                fcf,
                List.of(),
                (virtualCluster1, upstreamNodes) -> null,
                new ApiVersionsIntersectFilter(apiVersionsService),
                new ApiVersionsDowngradeFilter(apiVersionsService));
        final NetFilter.NetFilterContext netFilterContext = mock(NetFilter.NetFilterContext.class);

        // When
        initalizerNetFilter.selectServer(netFilterContext);

        // Then
        verify(fcf).createFilters(any(FilterFactoryContext.class), any(List.class));
    }

    @Test
    void shouldInitialiseTlsChannel() {
        // Given
        kafkaProxyInitializer = createKafkaProxyInitializer(true, (endpoint, sniHostname) -> bindingStage, Map.of());

        // When
        kafkaProxyInitializer.initChannel(channel);

        // Then
        verify(channelPipeline).addLast(anyString(), isA(SniHandler.class));
        assertErrorHandlerAdded();
    }

    @Test
    void shouldCloseConnectionOnUnrecognizedSniHostName() {
        // Given
        var endpointBindingResolver = mock(EndpointBindingResolver.class);
        var embeddedChannel = new EmbeddedChannel(serverSocketChannel, DefaultChannelId.newInstance(), true, false);
        kafkaProxyInitializer = createKafkaProxyInitializer(true, endpointBindingResolver, Map.of());
        kafkaProxyInitializer.initChannel(embeddedChannel);
        when(endpointBindingResolver.resolve(any(), eq("chat4.leancloud.cn"))).thenReturn(CompletableFuture.failedStage(new EndpointResolutionException("not resolved")));

        // lifted from the Netty tests
        // hex dump of a client hello packet, which contains hostname "CHAT4.LEANCLOUD.CN"
        String tlsHandshakeMessageHex1 = "16030100";
        // part 2
        String tlsHandshakeMessageHex = "c6010000c20303bb0855d66532c05a0ef784f7c384feeafa68b3" +
                "b655ac7288650d5eed4aa3fb52000038c02cc030009fcca9cca8ccaac02b" +
                "c02f009ec024c028006bc023c0270067c00ac0140039c009c0130033009d" +
                "009c003d003c0035002f00ff010000610000001700150000124348415434" +
                "2e4c45414e434c4f55442e434e000b000403000102000a000a0008001d00" +
                "170019001800230000000d0020001e060106020603050105020503040104" +
                "0204030301030203030201020202030016000000170000";
        assertThat(embeddedChannel.isOpen())
                .isTrue();

        // When
        embeddedChannel.writeInbound(Unpooled.wrappedBuffer(StringUtil.decodeHexDump(tlsHandshakeMessageHex1)),
                Unpooled.wrappedBuffer(StringUtil.decodeHexDump(tlsHandshakeMessageHex)));

        // Then
        assertThat(embeddedChannel.isOpen())
                .isFalse();
    }

    @Test
    void testLoggingErrorHandlerPreventsExceptionPropagatingToChannel() {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        embeddedChannel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                throw new RuntimeException("failed to handle message: " + msg);
            }
        });
        embeddedChannel.pipeline().addLast(new KafkaProxyInitializer.LoggingInboundErrorHandler());
        embeddedChannel.writeInbound("arbitrary");
        assertThatCode(embeddedChannel::checkException).doesNotThrowAnyException();
    }

    private @NonNull KafkaProxyInitializer createKafkaProxyInitializer(boolean tls,
                                                                       EndpointBindingResolver bindingResolver,
                                                                       Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> authnMechanismHandlers) {
        return new KafkaProxyInitializer(filterChainFactory,
                pfr,
                tls,
                bindingResolver,
                (virtualCluster, upstreamNodes) -> null,
                false,
                authnMechanismHandlers, new ApiVersionsServiceImpl());
    }

    private void assertErrorHandlerAdded() {
        verify(channelPipeline).addLast(anyString(), isA(KafkaProxyInitializer.LoggingInboundErrorHandler.class));
    }

    private void verifyFrameLoggerAdded(InOrder orderedVerifyer) {
        orderedVerifyer.verify(channelPipeline).addLast(eq("frameLogger"), any(LoggingHandler.class));
    }

    private void verifyErrorHandlerAdded(InOrder orderedVerifyer) {
        orderedVerifyer.verify(channelPipeline).addLast(eq(LOGGING_INBOUND_ERROR_HANDLER_NAME), isA(KafkaProxyInitializer.LoggingInboundErrorHandler.class));
    }

    private void verifyFrontendHandlerAdded(InOrder orderedVerifyer) {
        orderedVerifyer.verify(channelPipeline).addLast(eq("netHandler"), any(KafkaProxyFrontendHandler.class));
    }

    private void verifyErrorHandlerRemoved(InOrder orderedVerifyer) {
        orderedVerifyer.verify(channelPipeline).remove(LOGGING_INBOUND_ERROR_HANDLER_NAME);
    }

    private void verifyNetworkLoggerAdded(InOrder orderedVerifyer) {
        orderedVerifyer.verify(channelPipeline).addLast(eq("networkLogger"), any(LoggingHandler.class));
    }

    private void verifyEncoderAndOrdererAdded(InOrder orderedVerifyer) {
        orderedVerifyer.verify(channelPipeline).addLast(eq("requestDecoder"), any(ByteToMessageDecoder.class));
        orderedVerifyer.verify(channelPipeline).addLast(eq("responseEncoder"), any(MessageToByteEncoder.class));
        orderedVerifyer.verify(channelPipeline).addLast(eq("downstreamMetrics"), any(ChannelHandler.class));
        orderedVerifyer.verify(channelPipeline).addLast(eq("deprecatedDownstreamMetrics"), any(ChannelHandler.class));
        orderedVerifyer.verify(channelPipeline).addLast(eq("responseOrderer"), any(ResponseOrderer.class));
    }
}
