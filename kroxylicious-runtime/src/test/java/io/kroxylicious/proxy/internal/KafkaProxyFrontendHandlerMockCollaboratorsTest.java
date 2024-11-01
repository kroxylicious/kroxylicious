/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
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
import io.kroxylicious.proxy.model.VirtualCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProxyFrontendHandlerMockCollaboratorsTest {

    public static final String SOURCE_ADDRESS = "1.1.1.1";
    public static final HAProxyMessage HA_PROXY_MESSAGE = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
            SOURCE_ADDRESS, "1.0.0.1", 18466, 9090);
    @Mock
    NetFilter netFilter;

    @Mock
    VirtualCluster vc;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ChannelHandlerContext clientCtx;

    @Mock
    ProxyChannelStateMachine proxyChannelStateMachine;

    @Test
    void channelActive() throws Exception {
        // Given
        SaslDecodePredicate dp = new SaslDecodePredicate(false);
        KafkaProxyFrontendHandler handler = new KafkaProxyFrontendHandler(
                netFilter,
                dp,
                vc,
                proxyChannelStateMachine);

        // When
        handler.channelActive(clientCtx);

        // Then
        verify(proxyChannelStateMachine).onClientActive(handler);
        verifyNoInteractions(netFilter);
    }

    @Test
    void channelRead() {
        // Given
        SaslDecodePredicate dp = new SaslDecodePredicate(false);
        HAProxyMessage msg = new HAProxyMessage(
                HAProxyProtocolVersion.V2,
                HAProxyCommand.PROXY,
                HAProxyProxiedProtocol.TCP4,
                "1.1.1.1",
                "2.2.2.2",
                1234,
                4567);
        KafkaProxyFrontendHandler handler = new KafkaProxyFrontendHandler(
                netFilter,
                dp,
                vc,
                proxyChannelStateMachine);

        // When
        handler.channelRead(clientCtx, msg);

        // Then
        verify(proxyChannelStateMachine).onClientRequest(dp, msg);
        verifyNoInteractions(netFilter);
    }

    @Test
    void inSelectingServer() {
        // Given
        SaslDecodePredicate dp = new SaslDecodePredicate(false);
        KafkaProxyFrontendHandler handler = new KafkaProxyFrontendHandler(
                netFilter,
                dp,
                vc,
                proxyChannelStateMachine);

        // When
        handler.inSelectingServer();

        // Then
        verify(netFilter).selectServer(handler);
        verify(proxyChannelStateMachine).assertIsConnecting(anyString());
    }

    @Test
    void shouldReturnClientHostFromChannelInSelectingServer() throws Exception {
        // Given
        SaslDecodePredicate dp = new SaslDecodePredicate(false);
        KafkaProxyFrontendHandler handler = new KafkaProxyFrontendHandler(
                netFilter,
                dp,
                vc,
                proxyChannelStateMachine);
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(proxyChannelStateMachine.state()).thenReturn(new ProxyChannelState.SelectingServer(null, "SnappyKafka", "0.1.5"));
        when(channel.remoteAddress()).thenReturn(new LocalAddress("127.0.0.1"));

        // When
        final String actualClientHost = handler.clientHost();

        // Then
        assertThat(actualClientHost).isEqualTo("local:127.0.0.1");
    }

    @Test
    void shouldReturnClientHostFromHaProxyInSelectingServer() throws Exception {
        // Given
        SaslDecodePredicate dp = new SaslDecodePredicate(false);
        KafkaProxyFrontendHandler handler = new KafkaProxyFrontendHandler(
                netFilter,
                dp,
                vc,
                proxyChannelStateMachine);
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(proxyChannelStateMachine.state()).thenReturn(new ProxyChannelState.SelectingServer(HA_PROXY_MESSAGE, "SnappyKafka", "0.1.5"));

        // When
        final String actualClientHost = handler.clientHost();

        // Then
        assertThat(actualClientHost).isEqualTo(SOURCE_ADDRESS);
        verifyNoInteractions(channel);
    }

    @Test
    void shouldCloseConnectionOnClientHostOutsideOfSelectingServer() throws Exception {
        // Given
        SaslDecodePredicate dp = new SaslDecodePredicate(false);
        KafkaProxyFrontendHandler handler = new KafkaProxyFrontendHandler(
                netFilter,
                dp,
                vc,
                proxyChannelStateMachine);
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(proxyChannelStateMachine.state()).thenReturn(new ProxyChannelState.Forwarding(HA_PROXY_MESSAGE, "SnappyKafka", "0.1.5"));

        // When
        final String actualClientHost = handler.clientHost();

        // Then
        assertThat(actualClientHost).isNull();
        verify(proxyChannelStateMachine).illegalState(anyString());
    }
}
