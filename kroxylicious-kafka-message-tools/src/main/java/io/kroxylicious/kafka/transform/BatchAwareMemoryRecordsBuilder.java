/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * <p>A builder of {@link MemoryRecords} that can cope with multiple batches.
 * (Apache Kafka's own {@link MemoryRecordsBuilder} assumes a single batch).</p>
 */
@NotThreadSafe
public class BatchAwareMemoryRecordsBuilder {

    private final ByteBufferOutputStream buffer;
    private @Nullable MemoryRecordsBuilder builder = null;
    private boolean closed = false;

    /**
     * Initialize a new instance, which will append into the given buffer.
     * @param buffer The buffer to use.
     */
    public BatchAwareMemoryRecordsBuilder(ByteBufferOutputStream buffer) {
        this.buffer = Objects.requireNonNull(buffer);
    }

    private boolean haveBatch() {
        return builder != null;
    }

    private void checkHasBatch() {
        if (!haveBatch()) {
            throw new IllegalStateException("You must start a batch");
        }
        if (builder.isClosed()) {
            throw new IllegalStateException("This builder has been built");
        }
    }

    private void checkIfClosed() {
        if (closed) {
            throw new IllegalStateException("Builder is closed");
        }
    }

    /**
     * Starts a batch
     *
     * @param magic
     * @param compression
     * @param timestampType
     * @param baseOffset
     * @param logAppendTime
     * @param producerId
     * @param producerEpoch
     * @param baseSequence
     * @param isTransactional
     * @param isControlBatch
     * @param partitionLeaderEpoch
     * @param deleteHorizonMs
     * @return this builder
     */
    public BatchAwareMemoryRecordsBuilder addBatch(byte magic,
                                                   Compression compression,
                                                   TimestampType timestampType,
                                                   long baseOffset,
                                                   long logAppendTime,
                                                   long producerId,
                                                   short producerEpoch,
                                                   int baseSequence,
                                                   boolean isTransactional,
                                                   boolean isControlBatch,
                                                   int partitionLeaderEpoch,
                                                   long deleteHorizonMs) {
        checkIfClosed();
        maybeAppendCurrentBatch();
        // MRB respects the initial position() of buffer, so this doesn't overwrite anything already in buffer
        builder = new MemoryRecordsBuilder(buffer,
                magic,
                compression,
                timestampType,
                baseOffset,
                logAppendTime,
                producerId,
                producerEpoch,
                baseSequence,
                isTransactional,
                isControlBatch,
                partitionLeaderEpoch,
                0, // TODO think about limiting the size that the buffer can grow to
                deleteHorizonMs);
        return this;
    }

    public BatchAwareMemoryRecordsBuilder addBatch(Compression compression,
                                                   TimestampType timestampType,
                                                   long baseOffset) {
        return addBatch(RecordBatch.CURRENT_MAGIC_VALUE,
                compression,
                timestampType,
                baseOffset,
                timestampType == TimestampType.LOG_APPEND_TIME ? System.currentTimeMillis() : RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                RecordBatch.NO_TIMESTAMP);
    }

    /**
     * Starts a batch, with batch parameters taken from the given {@code templateBatch}.
     * @param templateBatch The batch to use as a source of batch parameters
     * @return this builder
     */
    public BatchAwareMemoryRecordsBuilder addBatchLike(RecordBatch templateBatch) {
        TimestampType timestampType = templateBatch.timestampType();
        long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ? templateBatch.maxTimestamp() : RecordBatch.NO_TIMESTAMP;
        return addBatch(templateBatch.magic(),
                Compression.of(templateBatch.compressionType()).build(),
                timestampType,
                templateBatch.baseOffset(),
                logAppendTime,
                templateBatch.producerId(),
                templateBatch.producerEpoch(),
                templateBatch.baseSequence(),
                templateBatch.isTransactional(),
                templateBatch.isControlBatch(),
                templateBatch.partitionLeaderEpoch(),
                templateBatch.deleteHorizonMs().orElse(RecordBatch.NO_TIMESTAMP));
    }

    /**
     * Directly appends a batch, intended to be used for passing through unmodified batches. Writes
     * and closes the previous MemoryRecordBuilder batch to the stream if required
     * @param batch The batch to write to the buffer
     * @return this builder
     */
    public BatchAwareMemoryRecordsBuilder writeBatch(MutableRecordBatch batch) {
        checkIfClosed();
        if (haveBatch()) {
            appendCurrentBatch();
        }
        batch.writeTo(buffer);
        return this;
    }

    private void maybeAppendCurrentBatch() {
        if (haveBatch()) {
            appendCurrentBatch();
        }
    }

    private void appendCurrentBatch() {
        // Calling build will write out the (possibly compressed) batch bytes into this.buffer
        builder.build();
    }

    /**
     * Appends a record in the current batch
     * @param record The record to append
     * @return This builder
     */
    public BatchAwareMemoryRecordsBuilder append(SimpleRecord record) {
        checkIfClosed();
        checkHasBatch();
        builder.append(record);
        return this;
    }

    /**
     * Appends a record in the current batch
     * @param record The record to append
     * @return This builder
     */
    public BatchAwareMemoryRecordsBuilder append(Record record) {
        checkIfClosed();
        checkHasBatch();
        builder.append(record);
        return this;
    }

    /**
     * Appends a record using a different offset in the current batch.
     * @param offset The offset
     * @param record The record to append
     * @return This builder
     */
    public BatchAwareMemoryRecordsBuilder appendWithOffset(long offset, Record record) {
        checkIfClosed();
        checkHasBatch();
        builder.appendWithOffset(offset, record);
        return this;
    }

    /**
     * Append a new record at the given offset in the current batch.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return This builder
     */
    public BatchAwareMemoryRecordsBuilder appendWithOffset(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        checkIfClosed();
        checkHasBatch();
        builder.appendWithOffset(offset, timestamp, key, value, headers);
        return this;
    }

    /**
     * Append a new record at the given offset in the current batch.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return This builder
     */
    public BatchAwareMemoryRecordsBuilder appendWithOffset(long offset,
                                                           long timestamp,
                                                           ByteBuffer key,
                                                           ByteBuffer value,
                                                           Header[] headers) {
        checkIfClosed();
        checkHasBatch();
        builder.appendWithOffset(offset, timestamp, key, value, headers);
        return this;
    }

    public BatchAwareMemoryRecordsBuilder appendControlRecordWithOffset(long offset, SimpleRecord record) {
        checkIfClosed();
        checkHasBatch();
        builder.appendControlRecordWithOffset(offset, record);
        return this;
    }

    public BatchAwareMemoryRecordsBuilder appendEndTxnMarker(long timestamp,
                                                             EndTransactionMarker marker) {
        checkIfClosed();
        checkHasBatch();
        builder.appendEndTxnMarker(timestamp, marker);
        return this;
    }

    /**
     * Builds and returns the memory records.
     * Calling this multiple times is safe and will return a MemoryRecord with the same content,
     * <em>assuming the {@code buffer} argument passed to the
     * {@linkplain #BatchAwareMemoryRecordsBuilder(ByteBufferOutputStream) constructor}
     * has not been modified or used between calls</em>.
     * @return the memory records
     */
    public MemoryRecords build() {
        ByteBuffer recordsBuff;
        if (closed) {
            recordsBuff = this.buffer.buffer();
        }
        else {
            closed = true;
            maybeAppendCurrentBatch();
            ByteBuffer buf = this.buffer.buffer();
            buf.flip();
            recordsBuff = buf;
        }
        return MemoryRecords.readableRecords(recordsBuff);
    }

}
