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
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SniHandler;

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.ServiceBasedPluginFactoryRegistry;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.internal.filter.ApiVersionsDowngradeFilter;
import io.kroxylicious.proxy.internal.filter.ApiVersionsIntersectFilter;
import io.kroxylicious.proxy.internal.net.Endpoint;
import io.kroxylicious.proxy.internal.net.VirtualClusterBinding;
import io.kroxylicious.proxy.internal.net.VirtualClusterBindingResolver;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import static org.mockito.ArgumentMatchers.any;
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
    private VirtualClusterBinding vcb;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private EventLoop eventLoop;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ServerSocketChannel serverSocketChannel;

    @Captor
    ArgumentCaptor<ChannelInboundHandlerAdapter> plainChannelResolverCaptor;

    private ServiceBasedPluginFactoryRegistry pfr;
    private KafkaProxyInitializer kafkaProxyInitializer;
    private CompletionStage<VirtualClusterBinding> bindingStage;
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
        when(vcb.virtualClusterModel()).thenReturn(virtualClusterModel);
    }

    private VirtualClusterModel buildVirtualCluster(boolean logNetwork, boolean logFrames) {
        final Optional<Tls> tls = Optional.empty();
        return new VirtualClusterModel("testCluster",
                new TargetCluster("localhost:9090", tls),
                mock(ClusterNetworkAddressConfigProvider.class),
                tls,
                logNetwork,
                logFrames);
    }

    @Test
    void shouldInitialisePlainChannel() {
        // Given
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                false,
                (endpoint, sniHostname) -> bindingStage,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of(), new ApiVersionsServiceImpl());
        // When
        kafkaProxyInitializer.initChannel(channel);

        // Then
        verify(channelPipeline).addLast(isA(ChannelInboundHandlerAdapter.class));
        verify(channelPipeline, times(0)).addLast(isA(SniHandler.class));
    }

    @Test
    void shouldResolveWhenPlainChannelActivated() throws Exception {
        // Given
        final VirtualClusterBindingResolver virtualClusterBindingResolver = mock(VirtualClusterBindingResolver.class);
        when(virtualClusterBindingResolver.resolve(any(Endpoint.class), isNull())).thenReturn(bindingStage);
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                false,
                virtualClusterBindingResolver,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of(), new ApiVersionsServiceImpl());
        when(channelPipeline.addLast(plainChannelResolverCaptor.capture())).thenReturn(channelPipeline);

        kafkaProxyInitializer.initChannel(channel);
        final ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);

        // When
        plainChannelResolverCaptor.getValue().channelActive(channelHandlerContext);

        // Then
        verify(virtualClusterBindingResolver).resolve(any(Endpoint.class), isNull());
    }

    @Test
    void shouldRemovePlainChannelInitializerOnceComplete() throws Exception {
        // Given
        final VirtualClusterBindingResolver virtualClusterBindingResolver = mock(VirtualClusterBindingResolver.class);
        when(virtualClusterBindingResolver.resolve(any(Endpoint.class), isNull())).thenReturn(bindingStage);
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                false,
                virtualClusterBindingResolver,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of(), new ApiVersionsServiceImpl());
        when(channelPipeline.addLast(plainChannelResolverCaptor.capture())).thenReturn(channelPipeline);

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
        when(vcb.virtualClusterModel())
                .thenReturn(virtualClusterModel);
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                tls,
                (endpoint, sniHostname) -> bindingStage,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of(), new ApiVersionsServiceImpl());

        // When
        kafkaProxyInitializer.addHandlers(channel, vcb);

        // Then
        final InOrder verifyer = inOrder(channelPipeline);
        verifyer.verify(channelPipeline).addLast(eq("requestDecoder"), any(ByteToMessageDecoder.class));
        verifyer.verify(channelPipeline).addLast(eq("responseEncoder"), any(MessageToByteEncoder.class));
        verifyer.verify(channelPipeline).addLast(eq("responseOrderer"), any(ResponseOrderer.class));
        verifyer.verify(channelPipeline).addLast(eq("netHandler"), any(KafkaProxyFrontendHandler.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldAddFrameLoggerOnBindingComplete(boolean tls) {
        // Given
        virtualClusterModel = buildVirtualCluster(false, true);
        when(vcb.virtualClusterModel()).thenReturn(virtualClusterModel);
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                tls,
                (endpoint, sniHostname) -> bindingStage,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of(), new ApiVersionsServiceImpl());

        // When
        kafkaProxyInitializer.addHandlers(channel, vcb);

        // Then
        verify(channelPipeline).addLast(eq("frameLogger"), any(LoggingHandler.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldAddNetworkLoggerOnBindingComplete(boolean tls) {
        // Given
        virtualClusterModel = buildVirtualCluster(true, false);
        when(vcb.virtualClusterModel()).thenReturn(virtualClusterModel);
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                tls,
                (endpoint, sniHostname) -> bindingStage,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of(), new ApiVersionsServiceImpl());

        // When
        kafkaProxyInitializer.addHandlers(channel, vcb);

        // Then
        verify(channelPipeline).addLast(eq("networkLogger"), any(LoggingHandler.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldAddAuthnHandlersOnBindingComplete(boolean tls) {
        // Given
        virtualClusterModel = buildVirtualCluster(true, false);
        when(vcb.virtualClusterModel()).thenReturn(virtualClusterModel);
        final AuthenticateCallbackHandler plainHandler = mock(AuthenticateCallbackHandler.class);
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                tls,
                (endpoint, sniHostname) -> bindingStage,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of(KafkaAuthnHandler.SaslMechanism.PLAIN, plainHandler), new ApiVersionsServiceImpl());

        // When
        kafkaProxyInitializer.addHandlers(channel, vcb);

        // Then
        verify(channelPipeline).addLast(any(KafkaAuthnHandler.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldNotAddAuthnHandlersWithoutConfiguredMechanism(boolean tls) {
        // Given
        virtualClusterModel = buildVirtualCluster(true, false);
        when(vcb.virtualClusterModel()).thenReturn(virtualClusterModel);
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                tls,
                (endpoint, sniHostname) -> bindingStage,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of(), new ApiVersionsServiceImpl());

        // When
        kafkaProxyInitializer.addHandlers(channel, vcb);

        // Then
        verify(channelPipeline, times(0)).addLast(any(KafkaAuthnHandler.class));
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
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                true,
                (endpoint, sniHostname) -> bindingStage,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of(), new ApiVersionsServiceImpl());

        // When
        kafkaProxyInitializer.initChannel(channel);

        // Then
        verify(channelPipeline).addLast(isA(SniHandler.class));
    }
}
