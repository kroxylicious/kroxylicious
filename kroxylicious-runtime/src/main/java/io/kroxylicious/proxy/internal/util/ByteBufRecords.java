/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.util.Objects;

public class ByteBufRecords extends AbstractRecords {

    private final ByteBuf buf;

    private final Iterable<MutableRecordBatch> batches = this::batchIterator;

    private ByteBufRecords(ByteBuf buf) {
        Objects.requireNonNull(buf, "buffer should not be null");
        this.buf = buf.asReadOnly();
    }

    @Override
    public int sizeInBytes() {
        return buf.writerIndex();
    }

    @Override
    public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
        if (((long) position) + length > buf.writerIndex())
            throw new IllegalArgumentException("position+length should not be greater than buf.writerIndex(), position: "
                    + position + ", length: " + length + ", buf.writerIndex(): " + buf.writerIndex());

        return (int) channel.write(buf.slice(position, length).nioBuffers());
    }

    @Override
    public ConvertedRecords<ByteBufRecords> downConvert(byte toMagic, long firstOffset, Time time) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AbstractIterator<MutableRecordBatch> batchIterator() {
        return MemoryRecords.readableRecords(buf.nioBuffer()).batchIterator();
    }

    /**
     * Get the byte buffer that backs this instance for reading.
     */
    public ByteBuf buf() {
        return buf.duplicate();
    }

    @Override
    public Iterable<MutableRecordBatch> batches() {
        return batches;
    }

    @Override
    public String toString() {
        return "ByteBufRecords(size=" + sizeInBytes() +
                ", buf=" + buf +
                ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ByteBufRecords that = (ByteBufRecords) o;

        return buf.equals(that.buf);
    }

    @Override
    public int hashCode() {
        return buf.hashCode();
    }

    public static ByteBufRecords readableRecords(ByteBuf buf) {
        return new ByteBufRecords(buf);
    }
}
