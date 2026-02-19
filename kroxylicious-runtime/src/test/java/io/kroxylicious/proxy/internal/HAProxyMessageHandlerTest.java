/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class HAProxyMessageHandlerTest {

    private static final HAProxyMessage HA_PROXY_MESSAGE = new HAProxyMessage(
            HAProxyProtocolVersion.V2,
            HAProxyCommand.PROXY,
            HAProxyProxiedProtocol.TCP4,
            "192.168.1.100",
            "10.0.0.1",
            54321,
            9092);

    @Mock
    private ProxyChannelStateMachine proxyChannelStateMachine;

    @Mock
    private ChannelHandlerContext ctx;

    private KafkaSession kafkaSession;
    private HAProxyMessageHandler handler;

    @BeforeEach
    void setUp() {
        kafkaSession = new KafkaSession(KafkaSessionState.ESTABLISHING);
        handler = new HAProxyMessageHandler(kafkaSession, proxyChannelStateMachine);
    }

    @Test
    void shouldStoreHAProxyMessageInKafkaSession() throws Exception {
        // When
        handler.channelRead(ctx, HA_PROXY_MESSAGE);

        // Then
        assertThat(kafkaSession.haProxyMessage()).isSameAs(HA_PROXY_MESSAGE);
    }

    @Test
    void shouldForwardHAProxyMessageToStateMachine() throws Exception {
        // When
        handler.channelRead(ctx, HA_PROXY_MESSAGE);

        // Then - session is populated and state machine is signalled
        assertThat(kafkaSession.haProxyMessage()).isSameAs(HA_PROXY_MESSAGE);
        verify(proxyChannelStateMachine).onClientRequest(HA_PROXY_MESSAGE);
        verifyNoMoreInteractions(proxyChannelStateMachine);
        // Should NOT propagate to next handler
        verifyNoInteractions(ctx);
    }

    @Test
    void shouldPassThroughNonHAProxyMessages() throws Exception {
        // Given
        DecodedRequestFrame<?> kafkaFrame = mock(DecodedRequestFrame.class);

        // When
        handler.channelRead(ctx, kafkaFrame);

        // Then
        // Should NOT interact with state machine for non-HAProxy messages
        verifyNoInteractions(proxyChannelStateMachine);
        // Should propagate to next handler
        verify(ctx).fireChannelRead(kafkaFrame);
    }

    @Test
    void shouldPassThroughOtherObjectTypes() throws Exception {
        // Given
        Object someOtherMessage = new Object();

        // When
        handler.channelRead(ctx, someOtherMessage);

        // Then
        verifyNoInteractions(proxyChannelStateMachine);
        verify(ctx).fireChannelRead(someOtherMessage);
    }
}
