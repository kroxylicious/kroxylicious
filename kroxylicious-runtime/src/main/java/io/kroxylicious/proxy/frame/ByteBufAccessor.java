/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import java.nio.ByteBuffer;

import io.kroxylicious.kafka.common.protocol.Readable;
import io.kroxylicious.kafka.common.protocol.Writable;

import io.netty.buffer.ByteBuf;

/**
 * Provides read and write access to byte buffer for serializing frames.
 */
public interface ByteBufAccessor extends Readable, Writable {

    @Override
    byte readByte();

    @Override
    short readShort();

    @Override
    int readInt();

    @Override
    long readLong();

    @Override
    double readDouble();

    @Override
    byte[] readArray(int length);

    @Override
    int readUnsignedVarint();

    @Override
    ByteBuffer readByteBuffer(int length);

    @Override
    int readVarint();

    @Override
    long readVarlong();

    @Override
    int remaining();

    /**
     * The current reader index of the underlying buffer.
     * @return The reader index.
     */
    int readerIndex();

    /**
     * Sets the reader index of the underlying buffer.
     * @param readerIndex The new reader index.
     */
    void readerIndex(int readerIndex);

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
     * Ensures the underlying buffer has capacity for at least the given number of writable bytes.
     * @param encodedSize The number of bytes that need to be writable.
     */
    void ensureWritable(int encodedSize);

    /**
     * The current writer index of the underlying buffer.
     * @return The writer index.
     */
    int writerIndex();

    /**
     * Transfers bytes from the given buffer to the underlying buffer.
     * @param buf The buffer to read from.
     * @param length The number of bytes to transfer.
     */
    void writeBytes(ByteBuf buf, int length);
}
