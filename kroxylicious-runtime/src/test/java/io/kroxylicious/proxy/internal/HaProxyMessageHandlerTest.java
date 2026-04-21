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
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.net.HaProxyContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class HaProxyMessageHandlerTest {

    private static HAProxyMessage newHAProxyMessage() {
        return new HAProxyMessage(
                HAProxyProtocolVersion.V2,
                HAProxyCommand.PROXY,
                HAProxyProxiedProtocol.TCP4,
                "192.168.1.100",
                "10.0.0.1",
                54321,
                9092);
    }

    @Mock
    private KafkaSession kafkaSession;

    @Mock
    private ChannelHandlerContext ctx;

    private HaProxyMessageHandler handler;

    @BeforeEach
    void setUp() {
        handler = new HaProxyMessageHandler(kafkaSession);
    }

    @Test
    void shouldStoreHaProxyContextInSession() throws Exception {
        // When
        handler.channelRead(ctx, newHAProxyMessage());

        // Then - context is stored in kafka session
        verify(kafkaSession).setHaProxyContext(any(HaProxyContext.class));
        verifyNoMoreInteractions(kafkaSession);
    }

    @Test
    void shouldReleaseHaProxyMessageAfterProcessing() {
        // Given
        HAProxyMessage message = newHAProxyMessage();
        assertThat(message.refCnt()).isEqualTo(1);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound(message);

        // Then - SimpleChannelInboundHandler must release the ref-counted message
        assertThat(message.refCnt()).isZero();
        assertThat((Object) channel.readInbound()).isNull();
        verify(kafkaSession).setHaProxyContext(any(HaProxyContext.class));
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldPassThroughNonHAProxyMessages() throws Exception {
        // Given
        DecodedRequestFrame<?> kafkaFrame = mock(DecodedRequestFrame.class);

        // When
        handler.channelRead(ctx, kafkaFrame);

        // Then
        // Should NOT interact with session for non-HaProxy messages
        verifyNoInteractions(kafkaSession);
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
        verifyNoInteractions(kafkaSession);
        verify(ctx).fireChannelRead(someOtherMessage);
    }
}
