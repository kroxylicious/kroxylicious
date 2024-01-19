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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
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
import io.kroxylicious.proxy.internal.net.VirtualClusterBinding;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
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
    private ServerSocketChannel serverSocketChannel;

    private ServiceBasedPluginFactoryRegistry pfr;
    private KafkaProxyInitializer kafkaProxyInitializer;
    private CompletionStage<VirtualClusterBinding> bindingStage;
    private VirtualCluster virtualCluster;
    private FilterChainFactory filterChainFactory;

    @BeforeEach
    void setUp() {
        virtualCluster = buildVirtualCluster(false, false);
        pfr = new ServiceBasedPluginFactoryRegistry();
        bindingStage = CompletableFuture.completedStage(vcb);
        filterChainFactory = new FilterChainFactory(pfr, List.of());
        final InetSocketAddress localhost = new InetSocketAddress(0);
        when(channel.pipeline()).thenReturn(channelPipeline);
        when(channel.parent()).thenReturn(serverSocketChannel);
        when(channel.localAddress()).thenReturn(InetSocketAddress.createUnresolved("localhost", 9099));

        when(serverSocketChannel.localAddress()).thenReturn(localhost);
        when(vcb.virtualCluster()).thenReturn(virtualCluster);
    }

    private VirtualCluster buildVirtualCluster(boolean logNetwork, boolean logFrames) {
        final Optional<Tls> tls = Optional.empty();
        return new VirtualCluster("testCluster",
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
                Map.of());
        // When
        kafkaProxyInitializer.initChannel(channel);

        // Then
        verify(channelPipeline).addLast(isA(ChannelInboundHandlerAdapter.class));
        verify(channelPipeline, times(0)).addLast(isA(SniHandler.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldAddCommonHandlersOnBindingComplete(boolean tls) {
        // Given
        when(vcb.virtualCluster())
                .thenReturn(virtualCluster);
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                tls,
                (endpoint, sniHostname) -> bindingStage,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of());

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
        virtualCluster = buildVirtualCluster(false, true);
        when(vcb.virtualCluster()).thenReturn(virtualCluster);
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                tls,
                (endpoint, sniHostname) -> bindingStage,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of());

        // When
        kafkaProxyInitializer.addHandlers(channel, vcb);

        // Then
        verify(channelPipeline).addLast(eq("frameLogger"), any(LoggingHandler.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldAdNetworkLoggerOnBindingComplete(boolean tls) {
        // Given
        virtualCluster = buildVirtualCluster(true, false);
        when(vcb.virtualCluster()).thenReturn(virtualCluster);
        kafkaProxyInitializer = new KafkaProxyInitializer(filterChainFactory,
                pfr,
                tls,
                (endpoint, sniHostname) -> bindingStage,
                (virtualCluster, upstreamNodes) -> null,
                false,
                Map.of());

        // When
        kafkaProxyInitializer.addHandlers(channel, vcb);

        // Then
        verify(channelPipeline).addLast(eq("networkLogger"), any(LoggingHandler.class));
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
                Map.of());

        // When
        kafkaProxyInitializer.initChannel(channel);

        // Then
        verify(channelPipeline).addLast(isA(SniHandler.class));
    }
}
