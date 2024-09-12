/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ByteBufOutputStreamTest {

    @Test
    void testByteBufOutputStreamExpansion() throws IOException {
        try (ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(Unpooled.buffer(1))) {
            assertThat(byteBufOutputStream.remaining()).isEqualTo(1);
            byteBufOutputStream.write(5);
            assertThat(byteBufOutputStream.remaining()).isZero();
            byteBufOutputStream.write(3);
            assertThat(byteBufOutputStream.remaining()).isPositive();
        }
    }

    @Test
    void testInitialCapacity() throws IOException {
        try (ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(Unpooled.buffer(1))) {
            assertThat(byteBufOutputStream.initialCapacity()).isEqualTo(1);
        }
    }

    @Test
    void testGetNioBuffer() throws IOException {
        ByteBuf buffer = Unpooled.buffer(1);
        try (ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(buffer)) {
            assertThat(byteBufOutputStream.byteBuf()).isSameAs(buffer);
        }
    }

    @Test
    void testSettingPosition() throws IOException {
        try (ByteBufOutputStream out = new ByteBufOutputStream(Unpooled.buffer(64))) {
            out.write(new byte[]{ 1, 2, 3 });
            ByteBuf byteBuf = out.byteBuf();
            int writerIndex = byteBuf.writerIndex();
            assertThat(out.position()).isEqualTo(3);
            assertThat(out.limit()).isEqualTo(64);
            assertThat(out.remaining()).isEqualTo(61);
            out.position(2);
            assertThat(out.position()).isEqualTo(2);
            assertThat(out.limit()).isEqualTo(64);
            assertThat(out.remaining()).isEqualTo(62);
            assertThat(byteBuf.writerIndex()).isEqualTo(writerIndex - 1);
        }
    }

    @Test
    void testWriteMoreThanRemainingExpandsBuffer() throws IOException {
        try (ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(Unpooled.buffer(1))) {
            assertThat(byteBufOutputStream.remaining()).isEqualTo(1);
            byteBufOutputStream.write(new byte[]{ 1, 2 });
            ByteBuf byteBuf = byteBufOutputStream.byteBuf();
            assertThat(byteBuf.readByte()).isEqualTo((byte) 1);
            assertThat(byteBuf.readByte()).isEqualTo((byte) 2);
        }
    }

    @Test
    void testWriteMoreThanRemainingExpandsBufferUsingByteBuf() throws IOException {
        try (ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(Unpooled.buffer(1))) {
            assertThat(byteBufOutputStream.remaining()).isEqualTo(1);
            byteBufOutputStream.write(ByteBuffer.wrap(new byte[]{ 1, 2 }));
            ByteBuf byteBuf = byteBufOutputStream.byteBuf();
            assertThat(byteBuf.readByte()).isEqualTo((byte) 1);
            assertThat(byteBuf.readByte()).isEqualTo((byte) 2);
        }
    }

    @Test
    void testWriteMoreThanRemainingWithOffsetAndLengthExpandsBuffer() throws IOException {
        try (ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(Unpooled.buffer(1))) {
            assertThat(byteBufOutputStream.remaining()).isEqualTo(1);
            byteBufOutputStream.write(new byte[]{ 1, 2 }, 0, 2);
            ByteBuf byteBuf = byteBufOutputStream.byteBuf();
            assertThat(byteBuf.readByte()).isEqualTo((byte) 1);
            assertThat(byteBuf.readByte()).isEqualTo((byte) 2);
        }
    }

    @Test
    void testCompositeBufferWithMultipleUnderlyingBuffersNotSupported() {
        ByteBuf bufferA = Unpooled.buffer(1);
        ByteBuf bufferB = Unpooled.buffer(1);
        ByteBuf composite = new CompositeByteBuf(new UnpooledByteBufAllocator(false), false, 2, bufferA, bufferB);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new ByteBufOutputStream(composite));
        assertThat(exception).hasMessageContaining("Composite buffers are not supported");
    }

}
