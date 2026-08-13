/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kroxylicious.kafka.common.record.internal;

import io.kroxylicious.kafka.common.errors.CorruptRecordException;
import io.kroxylicious.kafka.common.network.TransferableChannel;
import io.kroxylicious.kafka.common.utils.Utils;
import io.kroxylicious.kafka.common.utils.AbstractIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Objects;

/**
 * A {@link Records} implementation backed by a ByteBuffer. This is used only for reading or
 * modifying in-place an existing buffer of record batches. To create a new buffer see {@link MemoryRecordsBuilder},
 * or one of the {@link #builder(ByteBuffer, byte, Compression, TimestampType, long)} variants.
 */
public class MemoryRecords extends AbstractRecords {
    public static final MemoryRecords EMPTY = MemoryRecords.readableRecords(ByteBuffer.allocate(0));

    private final ByteBuffer buffer;

    private final Iterable<MutableRecordBatch> batches = this::batchIterator;

    private int validBytes = -1;

    // Construct a writable memory records
    private MemoryRecords(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer should not be null");
        this.buffer = buffer;
    }

    @Override
    public int sizeInBytes() {
        return buffer.limit();
    }

    @Override
    public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
        if (((long) position) + length > buffer.limit())
            throw new IllegalArgumentException("position+length should not be greater than buffer.limit(), position: "
                    + position + ", length: " + length + ", buffer.limit(): " + buffer.limit());

        return Utils.tryWriteTo(channel, position, length, buffer);
    }

    /**
     * Write all records to the given channel (including partial records).
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException For any IO errors writing to the channel
     */
    public int writeFullyTo(GatheringByteChannel channel) throws IOException {
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes())
            written += channel.write(buffer);
        buffer.reset();
        return written;
    }

    /**
     * The total number of bytes in this message set not including any partial, trailing messages. This
     * may be smaller than what is returned by {@link #sizeInBytes()}.
     * @return The number of valid bytes
     */
    public int validBytes() {
        if (validBytes >= 0)
            return validBytes;

        int bytes = 0;
        for (RecordBatch batch : batches())
            bytes += batch.sizeInBytes();

        this.validBytes = bytes;
        return bytes;
    }

    @Override
    public AbstractIterator<MutableRecordBatch> batchIterator() {
        return new RecordBatchIterator<>(new ByteBufferLogInputStream(buffer.duplicate(), Integer.MAX_VALUE));
    }

    /**
     * Validates the header of the first batch and returns batch size.
     * @return first batch size including LOG_OVERHEAD if buffer contains header up to
     *         magic byte, null otherwise
     * @throws CorruptRecordException if record size or magic is invalid
     */
    public Integer firstBatchSize() {
        if (buffer.remaining() < HEADER_SIZE_UP_TO_MAGIC)
            return null;
        return new ByteBufferLogInputStream(buffer, Integer.MAX_VALUE).nextBatchSize();
    }

    /**
     * Get the byte buffer that backs this instance for reading.
     */
    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    @Override
    public MemoryRecords slice(int position, int size) {
        if (position < 0)
            throw new IllegalArgumentException("Invalid position: " + position + " in read from " + this);
        if (position > buffer.limit())
            throw new IllegalArgumentException("Slice from position " + position + " exceeds end position of " + this);
        if (size < 0)
            throw new IllegalArgumentException("Invalid size: " + size + " in read from " + this);

        int availableBytes = Math.min(size, buffer.limit() - position);
        // As of now, clients module support Java11 hence can't use ByteBuffer::slice(position, size) method.
        // So we need to create a duplicate buffer and set the position and limit. Duplicate buffer
        // is backed by original bytes hence not the content but only the relative position and limit
        // are changed in the duplicate buffer. Once the position and limit are set, we can call the
        // slice method to get the sliced buffer, which is a backed by the original buffer with the
        // position reset to 0 and limit set to the size of the slice.
        ByteBuffer slicedBuffer = buffer.duplicate();
        slicedBuffer.position(position);
        slicedBuffer.limit(position + availableBytes);
        // Reset the position to 0 so that the sliced view has a relative position.
        slicedBuffer = slicedBuffer.slice();

        return readableRecords(slicedBuffer);
    }

    @Override
    public Iterable<MutableRecordBatch> batches() {
        return batches;
    }

    @Override
    public String toString() {
        return "MemoryRecords(size=" + sizeInBytes() +
                ", buffer=" + buffer +
                ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        MemoryRecords that = (MemoryRecords) o;

        return buffer.equals(that.buffer);
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    public static MemoryRecords readableRecords(ByteBuffer buffer) {
        return new MemoryRecords(buffer);
    }

}
