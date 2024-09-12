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

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaProxyBackendHandlerTest {

    @Mock
    KafkaProxyFrontendHandler kafkaProxyFrontendHandler;

    private Channel outboundChannel;
    private KafkaProxyBackendHandler kafkaProxyBackendHandler;
    private ChannelHandlerContext outboundContext;

    @BeforeEach
    void setUp() {
        Channel inboundChannel = new EmbeddedChannel();
        inboundChannel.pipeline().addFirst("dummy", new ChannelDuplexHandler());
        outboundChannel = new EmbeddedChannel();
        outboundChannel.pipeline().addFirst("dummy", new ChannelDuplexHandler());
        kafkaProxyBackendHandler = new KafkaProxyBackendHandler(kafkaProxyFrontendHandler, inboundChannel.pipeline().firstContext());
        outboundContext = outboundChannel.pipeline().firstContext();
    }

    @Test
    void shouldForwardChannelActiveToFrontEndHandler() throws Exception {
        // Given

        // When
        kafkaProxyBackendHandler.channelActive(outboundContext);

        // Then
        verify(kafkaProxyFrontendHandler).onUpstreamChannelActive(outboundContext);
    }

    @Test
    void shouldInformFrontendHandlerOnUnanticipatedException() {
        // Given
        RuntimeException kaboom = new RuntimeException("Kaboom");

        // When
        kafkaProxyBackendHandler.exceptionCaught(outboundContext, kaboom);

        // Then
        verify(kafkaProxyFrontendHandler).upstreamExceptionCaught(outboundContext, kaboom);
    }
}
