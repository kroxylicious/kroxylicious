/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import java.nio.ByteBuffer;

import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;

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

    int readerIndex();

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

    void ensureWritable(int encodedSize);

    int writerIndex();

    void writeBytes(ByteBuf buf, int length);
}
