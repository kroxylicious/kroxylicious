/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.errors.UnknownServerException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProxyBackendHandlerTest {

    // TODO nasty mocking our own class but needs too much wiring for now.
    @Mock
    KafkaProxyFrontendHandler frontendHandler;

    @Mock
    ChannelHandlerContext channelHandlerContext;

    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel(DefaultChannelId.newInstance());
        doCallRealMethod().when(frontendHandler).closeWith(any(), any());
        when(channelHandlerContext.channel()).thenReturn(channel);
    }

    @Test
    void shouldCloseWithForRegisteredException() {

        // Given
        final KafkaProxyBackendHandler kafkaProxyBackendHandler = new KafkaProxyBackendHandler(frontendHandler, channelHandlerContext);
        kafkaProxyBackendHandler.registerExceptionResponse(SSLHandshakeException.class, UnknownServerException::new);

        // When
        kafkaProxyBackendHandler.exceptionCaught(channelHandlerContext, new SSLHandshakeException("it went wrong"));

        // Then
        final Object outbound = channel.readOutbound();
        assertThat(outbound).isInstanceOf(UnknownServerException.class);
    }

    @Test
    void shouldUnwrapCauseToFindForRegisteredException() {

        // Given
        final KafkaProxyBackendHandler kafkaProxyBackendHandler = new KafkaProxyBackendHandler(frontendHandler, channelHandlerContext);
        kafkaProxyBackendHandler.registerExceptionResponse(SSLHandshakeException.class, UnknownServerException::new);

        // When
        kafkaProxyBackendHandler.exceptionCaught(channelHandlerContext, new DecoderException(new SSLHandshakeException("it went wrong")));

        // Then
        final Object outbound = channel.readOutbound();
        assertThat(outbound).isInstanceOf(UnknownServerException.class);
    }
}