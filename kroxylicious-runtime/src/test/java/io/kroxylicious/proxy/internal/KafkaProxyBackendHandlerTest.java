/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaProxyBackendHandlerTest {

    public static final PortPerBrokerClusterNetworkAddressConfigProvider ADDRESS_CONFIG_PROVIDER = new PortPerBrokerClusterNetworkAddressConfigProvider(
            new PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig(new HostPort("localhost", 9090), "broker-", 9190,
                    0, 10));
    @Mock
    ProxyChannelStateMachine proxyChannelStateMachine;

    private KafkaProxyBackendHandler kafkaProxyBackendHandler;
    private ChannelHandlerContext outboundContext;
    private Channel outboundChannel;

    @BeforeEach
    void setUp() {
        outboundChannel = new EmbeddedChannel();
        kafkaProxyBackendHandler = new KafkaProxyBackendHandler(proxyChannelStateMachine,
                new VirtualCluster("wibble", new TargetCluster("localhost:9090", Optional.empty()),
                        ADDRESS_CONFIG_PROVIDER, Optional.empty(), false, false));
        outboundChannel.pipeline().addFirst(kafkaProxyBackendHandler);
        outboundContext = outboundChannel.pipeline().firstContext();
    }

    @Test
    void shouldNotifyServerActiveOnPlainConnection() throws Exception {
        // Given

        // When
        kafkaProxyBackendHandler.channelActive(outboundContext);

        // Then
        verify(proxyChannelStateMachine).onServerActive();
    }

    @Test
    void shouldNotifyServerExceptionOnExceptionCaught() {
        // Given
        RuntimeException kaboom = new RuntimeException("Kaboom");

        // When
        kafkaProxyBackendHandler.exceptionCaught(outboundContext, kaboom);

        // Then
        verify(proxyChannelStateMachine).onServerException(kaboom);
    }

    @Test
    void shouldNotifyServerActiveOnTlsNegotiated() throws Exception {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);

        // When
        kafkaProxyBackendHandler.userEventTriggered(serverCtx, SslHandshakeCompletionEvent.SUCCESS);

        // Then
        verify(proxyChannelStateMachine).onServerActive();
    }

    @Test
    void shouldNotifyServerActiveOnFailedTls() throws Exception {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);
        SSLHandshakeException cause = new SSLHandshakeException("Oops!");

        // When
        kafkaProxyBackendHandler.userEventTriggered(serverCtx, new SslHandshakeCompletionEvent(cause));

        // Then
        verify(proxyChannelStateMachine).onServerException(cause);
    }

    @Test
    void shouldCloseDirectly() {
        // Given
        final AtomicBoolean flushed = new AtomicBoolean(false);
        final AtomicBoolean closed = new AtomicBoolean(false);
        outboundChannel.closeFuture().addListener(future -> closed.set(true));

        // When
        outboundChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener((ChannelFutureListener) future -> {
            flushed.set(true);
            future.channel().close();
        });

        // Then
        await().untilTrue(flushed);
        await().untilTrue(closed);
    }

    @Test
    void shouldCloseChannelWhenInClosingState() throws Exception {
        // Given
        final AtomicBoolean closed = new AtomicBoolean(false);
        outboundChannel.closeFuture().addListener(future -> closed.set(true));
        kafkaProxyBackendHandler.channelActive(outboundContext);

        // When
        kafkaProxyBackendHandler.inClosing();

        // Then
        await().untilTrue(closed);

        verify(proxyChannelStateMachine).onServerClosed();
        assertThat(outboundChannel.isActive()).isFalse();
        assertThat(outboundChannel.isOpen()).isFalse();
    }
}
