/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class KafkaMessageDecoderTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageDecoderTest.class);

    @Mock
    private Frame frame;
    @Mock
    private ChannelHandlerContext context;

    @Test
    void insufficientFrameBytesAvailable() throws IOException {
        AtomicBoolean read = new AtomicBoolean(false);
        KafkaMessageDecoder decoder = decoder((headerAndBodyBuf, length) -> {
            headerAndBodyBuf.readBytes(length);
            read.set(true);
            return frame;
        });
        ByteBuf buffer = Unpooled.buffer();
        try (ByteBufOutputStream bufOutputStream = new ByteBufOutputStream(buffer)) {
            bufOutputStream.writeInt(Integer.BYTES);
            bufOutputStream.writeByte(1); // simulate only first byte of frame available in the buffer
        }
        ArrayList<Object> out = new ArrayList<>();
        int originalReadIndex = buffer.readerIndex();
        decoder.decode(context, buffer, out);
        assertThat(out).isEmpty();
        assertThat(read).isFalse();
        assertThat(buffer.readerIndex()).isEqualTo(originalReadIndex);
    }

    @Test
    void dcodeHeaderAndBody() throws IOException {
        ByteBuf decodeHeaderAndBodyCaptor = Unpooled.buffer();
        KafkaMessageDecoder decoder = decoder((headerAndBodyBuf, length) -> {
            headerAndBodyBuf.readBytes(decodeHeaderAndBodyCaptor, length);
            return frame;
        });
        ByteBuf buffer = integerFrameBuffer(Integer.MAX_VALUE);
        ArrayList<Object> out = new ArrayList<>();
        decoder.decode(context, buffer, out);
        assertThat(out).containsExactly(frame);
        // assert decodeHeaderAndBody is passed a buffer with the message bytes ready to read
        assertThat(decodeHeaderAndBodyCaptor.readableBytes()).isEqualTo(Integer.BYTES);
        assertThat(decodeHeaderAndBodyCaptor.readInt()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void decodeHeaderAndBodyDoesntReadAllBytes() throws IOException {
        KafkaMessageDecoder decoder = decoder((headerAndBodyBuf, length) -> {
            headerAndBodyBuf.readBytes(length - 1); // doesn't read all the header and body bytes
            return frame;
        });
        ByteBuf buffer = integerFrameBuffer(Integer.MAX_VALUE);
        ArrayList<Object> out = new ArrayList<>();
        assertThatThrownBy(() -> decoder.decode(context, buffer, out)).isInstanceOf(KafkaCodecException.class)
                .hasMessage("Error decoding KafkaMessage").cause().isInstanceOf(KafkaCodecException.class)
                .hasMessageContaining("decodeHeaderAndBody did not read all of the buffer");
    }

    @Test
    void decodeMultipleFrames() throws IOException {
        List<ByteBuf> decodeHeaderAndBodyCaptors = new ArrayList<>();
        KafkaMessageDecoder decoder = decoder((headerAndBodyBuf, length) -> {
            ByteBuf buffer = Unpooled.buffer();
            headerAndBodyBuf.readBytes(buffer, length);
            decodeHeaderAndBodyCaptors.add(buffer);
            return frame;
        });
        ByteBuf buffer = Unpooled.buffer();
        try (ByteBufOutputStream bufOutputStream = new ByteBufOutputStream(buffer)) {
            writeIntFrame(bufOutputStream, Integer.MAX_VALUE);
            writeIntFrame(bufOutputStream, 999);
        }
        ArrayList<Object> out = new ArrayList<>();
        decoder.decode(context, buffer, out);
        assertThat(out).containsExactly(frame, frame);
        ListAssert<ByteBuf> byteBufListAssert = assertThat(decodeHeaderAndBodyCaptors).hasSize(2);
        byteBufListAssert.element(0).satisfies(byteBuf -> {
            assertThat(byteBuf.readableBytes()).isEqualTo(Integer.BYTES);
            assertThat(byteBuf.readInt()).isEqualTo(Integer.MAX_VALUE);
        });
        byteBufListAssert.element(1).satisfies(byteBuf -> {
            assertThat(byteBuf.readableBytes()).isEqualTo(Integer.BYTES);
            assertThat(byteBuf.readInt()).isEqualTo(999);
        });
    }

    KafkaMessageDecoder decoder(BiFunction<ByteBuf, Integer, Frame> frameDecoder) {
        return new KafkaMessageDecoder() {

            @Override
            protected Logger log() {
                return LOG;
            }

            @Override
            protected Frame decodeHeaderAndBody(ChannelHandlerContext ctx, ByteBuf in, int length) {
                return frameDecoder.apply(in, length);
            }
        };
    }

    private static ByteBuf integerFrameBuffer(int headerAndBody) throws IOException {
        ByteBuf buffer = Unpooled.buffer();
        try (ByteBufOutputStream bufOutputStream = new ByteBufOutputStream(buffer)) {
            writeIntFrame(bufOutputStream, headerAndBody);
        }
        return buffer;
    }

    private static void writeIntFrame(ByteBufOutputStream bufOutputStream, int headerAndBody) throws IOException {
        bufOutputStream.writeInt(Integer.BYTES);
        bufOutputStream.writeInt(headerAndBody);
    }

}
