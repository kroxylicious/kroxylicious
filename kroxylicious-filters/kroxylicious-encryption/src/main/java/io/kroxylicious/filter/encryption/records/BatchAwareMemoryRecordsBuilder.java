/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.records;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A builder of {@link MemoryRecords} that can cope with multiple batches.
 * (Apache Kafka's own {@link MemoryRecordsBuilder} assumes a single batch).
 */
public class BatchAwareMemoryRecordsBuilder {

    public static final Header[] EMPTY_HEADERS = new Header[0];

    private final ByteBufferOutputStream buffer;
    private MemoryRecordsBuilder builder = null;

    public BatchAwareMemoryRecordsBuilder(
                                          @NonNull ByteBufferOutputStream buffer) {
        this.buffer = Objects.requireNonNull(buffer);
    }

    private boolean haveBatch() {
        return builder != null;
    }

    private void checkHasBatch() {
        if (!haveBatch()) {
            throw new IllegalStateException("You must start a batch");
        }
    }

    /**
     * Starts a batch
     * @param magic
     * @param compressionType
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
    public @NonNull BatchAwareMemoryRecordsBuilder addBatch(byte magic,
                                                            CompressionType compressionType,
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
        maybeAppendCurrentBatch();
        // MRB respects the initial position() of buffer, so this doesn't overwrite anything already in buffer
        builder = new MemoryRecordsBuilder(buffer,
                magic,
                compressionType,
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

    public BatchAwareMemoryRecordsBuilder addBatch(CompressionType compressionType,
                                                   TimestampType timestampType,
                                                   long baseOffset) {
        return addBatch(RecordBatch.CURRENT_MAGIC_VALUE,
                compressionType,
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
    public @NonNull BatchAwareMemoryRecordsBuilder addBatchLike(RecordBatch templateBatch) {
        return addBatch(templateBatch.magic(),
                templateBatch.compressionType(),
                templateBatch.timestampType(),
                templateBatch.baseOffset(),
                0,
                templateBatch.producerId(),
                templateBatch.producerEpoch(),
                templateBatch.baseSequence(),
                templateBatch.isTransactional(),
                templateBatch.isControlBatch(),
                templateBatch.partitionLeaderEpoch(),
                templateBatch.deleteHorizonMs().orElse(RecordBatch.NO_TIMESTAMP));
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
    public @NonNull BatchAwareMemoryRecordsBuilder append(SimpleRecord record) {
        checkHasBatch();
        builder.append(record);
        return this;
    }

    /**
     * Appends a record in the current batch
     * @param record The record to append
     * @return This builder
     */
    public @NonNull BatchAwareMemoryRecordsBuilder append(Record record) {
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
    public @NonNull BatchAwareMemoryRecordsBuilder appendWithOffset(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
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
    public @NonNull BatchAwareMemoryRecordsBuilder appendWithOffset(long offset,
                                                                    long timestamp,
                                                                    ByteBuffer key,
                                                                    ByteBuffer value,
                                                                    Header[] headers) {
        checkHasBatch();
        builder.appendWithOffset(offset, timestamp, key, value, headers);
        return this;
    }

    public @NonNull BatchAwareMemoryRecordsBuilder appendControlRecordWithOffset(long offset, @NonNull SimpleRecord record) {
        checkHasBatch();
        builder.appendControlRecordWithOffset(offset, record);
        return this;
    }

    public @NonNull BatchAwareMemoryRecordsBuilder appendEndTxnMarker(long timestamp,
                                                                      @NonNull EndTransactionMarker marker) {
        checkHasBatch();
        builder.appendEndTxnMarker(timestamp, marker);
        return this;
    }

    /**
     * Builds the memory records
     * @return the memory records
     */
    public @NonNull MemoryRecords build() {
        maybeAppendCurrentBatch();
        return MemoryRecords.readableRecords(buffer.buffer().flip());
    }

}
