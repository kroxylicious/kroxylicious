/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.frame.OpaqueResponseFrame;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class KafkaProxyBackendHandlerTest {

    @Mock
    KafkaProxyFrontendHandler kafkaProxyFrontendHandler;

    private Channel outboundChannel;
    private KafkaProxyBackendHandler kafkaProxyBackendHandler;
    private KafkaProxyExceptionMapper exceptionMapper;
    private ChannelHandlerContext outboundContext;

    @BeforeEach
    void setUp() {
        Channel inboundChannel = new EmbeddedChannel();
        inboundChannel.pipeline().addFirst("dummy", new ChannelDuplexHandler());
        outboundChannel = new EmbeddedChannel();
        outboundChannel.pipeline().addFirst("dummy", new ChannelDuplexHandler());
        exceptionMapper = new KafkaProxyExceptionMapper();
        kafkaProxyBackendHandler = new KafkaProxyBackendHandler(kafkaProxyFrontendHandler, inboundChannel.pipeline().firstContext(), exceptionMapper);
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
    void shouldNotActIfExceptionRegistered() {
        // Given
        var resp = new OpaqueResponseFrame(Unpooled.EMPTY_BUFFER, 42, 0);
        exceptionMapper.registerExceptionResponse(RuntimeException.class, throwable -> Optional.of(resp));

        // When
        kafkaProxyBackendHandler.exceptionCaught(outboundContext, new RuntimeException("Kaboom"));

        // Then
        verifyNoInteractions(kafkaProxyFrontendHandler);
    }

    @Test
    void shouldCloseChannelOnUnanticipatedException() {
        // Given

        // When
        kafkaProxyBackendHandler.exceptionCaught(outboundContext, new RuntimeException("Kaboom"));

        // Then
        verify(kafkaProxyFrontendHandler).closeNoResponse(outboundChannel);
    }
}
