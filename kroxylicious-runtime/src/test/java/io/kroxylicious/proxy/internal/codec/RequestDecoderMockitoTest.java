/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.codec;

import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.buffer.ByteBuf;

import static io.kroxylicious.proxy.internal.codec.RequestDecoderTest.DECODE_EVERYTHING;
import static io.kroxylicious.proxy.internal.codec.RequestDecoderTest.getKafkaRequestDecoder;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RequestDecoderMockitoTest {

    private static final int SOCKET_MAX_FRAME_SIZE = 1024;

    private KafkaRequestDecoder kafkaRequestDecoder;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ByteBuf frameBuffer;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ByteBuf frameSlice;

    @BeforeEach
    void setUp() {
        kafkaRequestDecoder =  getKafkaRequestDecoder(DECODE_EVERYTHING, SOCKET_MAX_FRAME_SIZE);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRewindBufferIfNotEnoughBytesAvailable() {
        // Given
        int initialIndex = 11;
        when(frameBuffer.isReadable()).thenReturn(true);
        when(frameBuffer.readerIndex()).thenReturn(initialIndex);
        when(frameBuffer.readInt()).thenReturn(10);
        when(frameBuffer.readableBytes()).thenReturn(7);

        // When
        kafkaRequestDecoder.decode(null, frameBuffer, List.of());

        // Then
        verify(frameBuffer).readerIndex(eq(initialIndex));
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldThrowOnPartialIfReadingPartialFrame() {
        // Given
        int initialIndex = 11;
        when(frameBuffer.isReadable()).thenReturn(true);
        when(frameBuffer.readerIndex()).thenReturn(initialIndex);
        when(frameBuffer.readInt()).thenReturn(10);
        when(frameBuffer.readableBytes()).thenReturn(20);
        when(frameBuffer.readSlice(anyInt())).thenReturn(frameSlice);

        // When
        // Then
        Assertions.assertThatThrownBy(() -> kafkaRequestDecoder.decode(null, frameBuffer, new ArrayList<>()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("decodeHeaderAndBody did not read all of the buffer");

    }
}
