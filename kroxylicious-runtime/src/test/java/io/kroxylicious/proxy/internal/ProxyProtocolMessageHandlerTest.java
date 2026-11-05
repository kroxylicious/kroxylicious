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
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.net.ProxyProtocolContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ProxyProtocolMessageHandlerTest {

    @Mock
    private KafkaSession kafkaSession;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private ChannelPipeline pipeline;

    @Mock
    private DecodedRequestFrame<?> kafkaFrame;

    private ProxyProtocolMessageHandler handler;

    @BeforeEach
    void setUp() {
        handler = new ProxyProtocolMessageHandler(kafkaSession);
        when(ctx.pipeline()).thenReturn(pipeline);
    }

    private static HAProxyMessage haProxyMessage() {
        return new HAProxyMessage(HAProxyProtocolVersion.V1, HAProxyCommand.PROXY,
                HAProxyProxiedProtocol.TCP4, "192.168.1.100", "10.0.0.1", 54321, 9092);
    }

    @Test
    void shouldStoreProxyProtocolContextInSession() throws Exception {
        // When
        handler.channelRead(ctx, haProxyMessage());

        // Then
        verify(kafkaSession).sessionId(); // called by debug log
        verify(kafkaSession).setProxyProtocolContext(any(ProxyProtocolContext.class));
        verifyNoMoreInteractions(kafkaSession);
    }

    @Test
    void shouldRemoveSelfFromPipelineAfterProcessing() throws Exception {
        // When
        handler.channelRead(ctx, haProxyMessage());

        // Then
        verify(pipeline).remove(handler);
    }

    @Test
    void shouldNotPropagateHAProxyMessageDownPipeline() throws Exception {
        // When
        handler.channelRead(ctx, haProxyMessage());

        // Then - HAProxyMessage must not be passed to downstream Kafka handlers
        verify(ctx, never()).fireChannelRead(any(HAProxyMessage.class));
    }

    @Test
    void shouldReleaseHAProxyMessageAfterProcessing() throws Exception {
        // Given
        var haProxyMessage = haProxyMessage();

        // When
        handler.channelRead(ctx, haProxyMessage);

        // Then
        assertThat(haProxyMessage.refCnt()).isZero();
    }

    @Test
    void shouldPassThroughNonHAProxyMessages() throws Exception {
        // When
        handler.channelRead(ctx, kafkaFrame);

        // Then
        verifyNoInteractions(kafkaSession);
        verify(ctx).fireChannelRead(kafkaFrame);
    }

    @Test
    void shouldPassThroughOtherObjectTypes() throws Exception {
        // Given
        var someOtherMessage = new Object();

        // When
        handler.channelRead(ctx, someOtherMessage);

        // Then
        verifyNoInteractions(kafkaSession);
        verify(ctx).fireChannelRead(someOtherMessage);
    }
}
