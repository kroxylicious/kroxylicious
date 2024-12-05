/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.codec;

import java.nio.ByteBuffer;

import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;

/**
 * Provides write access to byte buffer for serializing frames.
 */
public interface ByteBufAccessor extends Writable, Readable {

    @Override
    void writeByte(byte val);

    @Override
    void writeShort(short val);

    @Override
    void writeInt(int val);

    @Override
    void writeLong(long val);

    @Override
    void writeDouble(double val);

    @Override
    void writeByteArray(byte[] arr);

    @Override
    void writeUnsignedVarint(int i);

    @Override
    void writeByteBuffer(ByteBuffer byteBuffer);

    @Override
    void writeVarint(int i);

    @Override
    void writeVarlong(long i);

    /**
     * Ensure underlying buffer is sized ready to be written to
     * @param encodedSize size we are about to write
     */
    void ensureWritable(int encodedSize);

    /**
     * Get writer index
     * @return writerIndex of underlying buffer
     */
    int writerIndex();

    /**
     * Get reader index
     * @return readerIndex of underlying buffer
     */
    int readerIndex();

    /**
     * set reader index of underlying buffer
     */
    void readerIndex(int index);
}
