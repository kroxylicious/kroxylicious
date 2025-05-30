/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.net.InetSocketAddress;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.local.LocalAddress;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;

import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProxyFrontendHandlerMockCollaboratorsTest {

    public static final String SOURCE_ADDRESS = "1.1.1.1";
    public static final int SOURCE_PORT = 18466;
    public static final HAProxyMessage HA_PROXY_MESSAGE = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
            SOURCE_ADDRESS, "1.0.0.1", SOURCE_PORT, 9090);
    public static final SaslDecodePredicate NO_SASL_DECODE_PREDICATE = new SaslDecodePredicate(false);
    @Mock
    NetFilter netFilter;

    @Mock
    VirtualClusterModel virtualCluster;

    @Mock
    EndpointBinding endpointBinding;

    @Mock
    EndpointGateway endpointGateway;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ChannelHandlerContext clientCtx;

    @Mock
    ProxyChannelStateMachine proxyChannelStateMachine;
    private KafkaProxyFrontendHandler handler;

    @BeforeEach
    void setUp() {
        when(endpointGateway.virtualCluster()).thenReturn(virtualCluster);
        when(endpointBinding.endpointGateway()).thenReturn(endpointGateway);
        handler = new KafkaProxyFrontendHandler(
                netFilter,
                NO_SASL_DECODE_PREDICATE,
                endpointBinding,
                proxyChannelStateMachine);
    }

    @Test
    void channelActive() throws Exception {
        // Given

        // When
        handler.channelActive(clientCtx);

        // Then
        verify(proxyChannelStateMachine).onClientActive(handler);
        verifyNoInteractions(netFilter);
    }

    @Test
    void channelRead() {
        // Given
        HAProxyMessage msg = new HAProxyMessage(
                HAProxyProtocolVersion.V2,
                HAProxyCommand.PROXY,
                HAProxyProxiedProtocol.TCP4,
                "1.1.1.1",
                "2.2.2.2",
                1234,
                4567);

        // When
        handler.channelRead(clientCtx, msg);

        // Then
        verify(proxyChannelStateMachine).onClientRequest(NO_SASL_DECODE_PREDICATE, msg);
        verifyNoInteractions(netFilter);
    }

    @Test
    void inSelectingServer() {
        // Given

        // When
        handler.inSelectingServer();

        // Then
        verify(netFilter).selectServer(handler);
        verify(proxyChannelStateMachine).assertIsConnecting(anyString());
    }

    @Test
    void shouldReturnClientHostFromChannelInSelectingServer() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(proxyChannelStateMachine.enforceInSelectingServer(anyString())).thenReturn(new ProxyChannelState.SelectingServer(null, "SnappyKafka", "0.1.5"));
        when(channel.remoteAddress()).thenReturn(new LocalAddress("127.0.0.1"));

        // When
        final String actualClientHost = handler.clientHost();

        // Then
        assertThat(actualClientHost).isEqualTo("local:127.0.0.1");
    }

    @Test
    void shouldReturnClientHostFromChannelSocketAddressInSelectingServer() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(proxyChannelStateMachine.enforceInSelectingServer(anyString())).thenReturn(new ProxyChannelState.SelectingServer(null, "SnappyKafka", "0.1.5"));
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress(SOURCE_ADDRESS, SOURCE_PORT));

        // When
        final String actualClientHost = handler.clientHost();

        // Then
        assertThat(actualClientHost).isEqualTo(SOURCE_ADDRESS);
    }

    @Test
    void shouldReturnClientHostFromHaProxyInSelectingServer() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(proxyChannelStateMachine.enforceInSelectingServer(anyString())).thenReturn(new ProxyChannelState.SelectingServer(HA_PROXY_MESSAGE, "SnappyKafka", "0.1.5"));

        // When
        final String actualClientHost = handler.clientHost();

        // Then
        assertThat(actualClientHost).isEqualTo(SOURCE_ADDRESS);
        verifyNoInteractions(channel);
    }

    @Test
    void shouldCloseConnectionOnClientHostOutsideOfSelectingServer() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        final ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        when(proxyChannelStateMachine.enforceInSelectingServer(messageCaptor.capture())).thenAnswer(invocation -> {
            throw new IllegalStateException(messageCaptor.getValue());
        });

        // When
        assertThatThrownBy(() -> handler.clientHost()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("NetFilterContext invoked in wrong session state");

        // Then
        verify(proxyChannelStateMachine).enforceInSelectingServer(anyString());
    }

    @Test
    void shouldReturnUnknownClientPortFromChannelInSelectingServer() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(proxyChannelStateMachine.enforceInSelectingServer(anyString())).thenReturn(new ProxyChannelState.SelectingServer(null, "SnappyKafka", "0.1.5"));
        when(channel.remoteAddress()).thenReturn(new LocalAddress("127.0.0.1"));

        // When
        final int actualClientPort = handler.clientPort();

        // Then
        assertThat(actualClientPort).isEqualTo(-1);
    }

    @Test
    void shouldReturnClientPortFromHaProxyInSelectingServer() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(proxyChannelStateMachine.enforceInSelectingServer(anyString())).thenReturn(new ProxyChannelState.SelectingServer(HA_PROXY_MESSAGE, "SnappyKafka", "0.1.5"));

        // When
        final int actualClientPort = handler.clientPort();

        // Then
        assertThat(actualClientPort).isEqualTo(SOURCE_PORT);
        verifyNoInteractions(channel);
    }

    @Test
    void shouldReturnClientPortFromChannelSocketAddressInSelectingServer() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(proxyChannelStateMachine.enforceInSelectingServer(anyString())).thenReturn(new ProxyChannelState.SelectingServer(null, "SnappyKafka", "0.1.5"));
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress(SOURCE_ADDRESS, SOURCE_PORT));

        // When
        final int actualClientPort = handler.clientPort();

        // Then
        assertThat(actualClientPort).isEqualTo(SOURCE_PORT);
    }

    @Test
    void shouldCloseConnectionOnClientPortOutsideOfSelectingServer() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        final ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        when(proxyChannelStateMachine.enforceInSelectingServer(messageCaptor.capture())).thenAnswer(invocation -> {
            throw new IllegalStateException(messageCaptor.getValue());
        });

        // When
        assertThatThrownBy(() -> handler.clientPort()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("NetFilterContext invoked in wrong session state");

        // Then
        verify(proxyChannelStateMachine).enforceInSelectingServer(messageCaptor.capture());
    }

    @Test
    void shouldNotifyStateMachineWhenChannelBecomesUnWriteable() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(channel.isWritable()).thenReturn(false);

        // When
        handler.channelWritabilityChanged(clientCtx);

        // Then
        verify(proxyChannelStateMachine).onClientUnwritable();
    }

    @Test
    void shouldNotifyStateMachineWhenChannelBecomesWriteable() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(channel.isWritable()).thenReturn(true);

        // When
        handler.channelWritabilityChanged(clientCtx);

        // Then
        verify(proxyChannelStateMachine).onClientWritable();
    }
}
