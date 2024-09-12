/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.record;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Utilities for easily creating Records, MemoryRecords etc, for use in tests
 */
public class RecordTestUtils {

    public static final byte DEFAULT_MAGIC_VALUE = RecordBatch.CURRENT_MAGIC_VALUE;
    public static final long DEFAULT_OFFSET = 0;
    public static final long DEFAULT_TIMESTAMP = 0;
    private static final byte[] DEFAULT_KEY_BYTES = null;
    private static final ByteBuffer DEFAULT_KEY_BUFFER = null;
    private static final String DEFAULT_KEY_STRING = null;

    private RecordTestUtils() {
    }

    /**
     * Return an array containing the same byte sequence as between {@code buffer}'s position and its limit,
     * leaving {@code buffer}'s position unchanged.
     * We don't use {@link ByteBuffer#array()} because that returns the whole backing array, and isn't available for all ByteBuffers.
     * @param buffer The buffer to get the bytes of
     * @return The bytes in the buffer
     */
    private static byte[] bytesOf(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        var result = new byte[buffer.remaining()];
        buffer.get(buffer.position(), result);
        return result;
    }

    public static String asHex(ByteBuffer buffer) {
        return HexFormat.of().formatHex(RecordTestUtils.bytesOf(buffer));
    }

    public static String asString(ByteBuffer buffer) {
        return buffer == null ? null : new String(bytesOf(buffer), StandardCharsets.UTF_8);
    }

    /**
     * Get a copy of the bytes contained in the given {@code record}'s value, without changing the
     * {@code record}'s {@link ByteBuffer#position() position}, {@link ByteBuffer#limit() limit} or {@link ByteBuffer#mark() mark}.
     * @param record The record
     * @return The records bytes
     */
    public static byte[] recordValueAsBytes(Record record) {
        return bytesOf(record.value());
    }

    public static String recordValueAsString(Record record) {
        return asString(record.value());
    }

    public static byte[] recordKeyAsBytes(Record record) {
        return bytesOf(record.key());
    }

    public static String recordKeyAsString(Record record) {
        return asString(record.key());
    }

    /**
     * Return a record with the given value and header.
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(
            byte[] value,
            Header... headers
    ) {
        return record(DEFAULT_MAGIC_VALUE, DEFAULT_OFFSET, DEFAULT_TIMESTAMP, DEFAULT_KEY_BYTES, value, headers);
    }

    /**
     * Return a Record with the given value and headers
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(
            ByteBuffer value,
            Header... headers
    ) {
        return record(DEFAULT_MAGIC_VALUE, DEFAULT_OFFSET, DEFAULT_TIMESTAMP, DEFAULT_KEY_BUFFER, value, headers);
    }

    /**
     * Return a Record with the given value, offset and headers
     * @param offset
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(
            long offset,
            ByteBuffer value,
            Header... headers
    ) {
        return record(DEFAULT_MAGIC_VALUE, offset, DEFAULT_TIMESTAMP, DEFAULT_KEY_BUFFER, value, headers);
    }

    /**
     * Return a Record with the given value and headers
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(
            String value,
            Header... headers
    ) {
        return record(DEFAULT_MAGIC_VALUE, DEFAULT_OFFSET, DEFAULT_TIMESTAMP, DEFAULT_KEY_STRING, value, headers);
    }

    /**
     * Return a Record with the given key, value and headers
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(
            byte[] key,
            byte[] value,
            Header... headers
    ) {
        return record(DEFAULT_MAGIC_VALUE, DEFAULT_OFFSET, DEFAULT_TIMESTAMP, key, value, headers);
    }

    /**
     * Return a Record with the given key, value and headers
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(
            String key,
            String value,
            Header... headers
    ) {
        return record(DEFAULT_MAGIC_VALUE, DEFAULT_OFFSET, DEFAULT_TIMESTAMP, key, value, headers);
    }

    /**
     * Return a Record with the given key, value, offset and headers
     * @param offset
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(
            long offset,
            String key,
            String value,
            Header... headers
    ) {
        return record(DEFAULT_MAGIC_VALUE, offset, DEFAULT_TIMESTAMP, key, value, headers);
    }

    /**
     * Return a Record with the given key, value and headers
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(
            ByteBuffer key,
            ByteBuffer value,
            Header... headers
    ) {
        return record(DEFAULT_MAGIC_VALUE, DEFAULT_OFFSET, DEFAULT_TIMESTAMP, key, value, headers);
    }

    /**
     * Return a Record with the given properties
     * @param magic
     * @param offset
     * @param timestamp
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(
            byte magic,
            long offset,
            long timestamp,
            byte[] key,
            byte[] value,
            Header... headers
    ) {
        // This is a bit of a rigmarole, but it ensures that calls to getSizeInBytes()
        // on the returned Record is actually correct
        MemoryRecords mr = singleElementMemoryRecords(magic, offset, timestamp, key, value, headers);
        return MemoryRecords.readableRecords(mr.buffer()).records().iterator().next();
    }

    /**
     * Return a Record with the given properties
     * @param magic
     * @param offset
     * @param timestamp
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(
            byte magic,
            long offset,
            long timestamp,
            String key,
            String value,
            Header... headers
    ) {
        // This is a bit of a rigmarole, but it ensures that calls to getSizeInBytes()
        // on the returned Record is actually correct
        MemoryRecords mr = singleElementMemoryRecords(magic, offset, timestamp, key, value, headers);
        return MemoryRecords.readableRecords(mr.buffer()).records().iterator().next();
    }

    /**
     * Return a Record with the given properties
     * @param magic
     * @param offset
     * @param timestamp
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(
            byte magic,
            long offset,
            long timestamp,
            ByteBuffer key,
            ByteBuffer value,
            Header... headers
    ) {
        // This is a bit of a rigmarole, but it ensures that calls to getSizeInBytes()
        // on the returned Record is actually correct
        MemoryRecords mr = singleElementMemoryRecords(magic, offset, timestamp, key, value, headers);
        return MemoryRecords.readableRecords(mr.buffer()).records().iterator().next();
    }

    public static Record record(long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        return record(RecordBatch.CURRENT_MAGIC_VALUE, offset, timestamp, key, value, headers);
    }

    public static Record record(long offset, long timestamp, String key, String value, Header[] headers) {
        return record(RecordBatch.CURRENT_MAGIC_VALUE, offset, timestamp, key, value, headers);
    }

    public static Record record(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        return record(RecordBatch.CURRENT_MAGIC_VALUE, offset, timestamp, key, value, headers);
    }

    /**
     * Return a singleton RecordBatch containing a single Record with the given key, value and headers.
     * The batch will use the current magic. The baseOffset and offset of the record will be 0
     * @see RecordTestUtils#singleElementRecordBatch(long, String, String, Header[])
     */
    public static MutableRecordBatch singleElementRecordBatch(
            String key,
            String value,
            Header... headers
    ) {
        return singleElementRecordBatch(DEFAULT_OFFSET, key, value, headers);
    }

    /**
     * Return a singleton RecordBatch containing a single Record with the given key, value and headers.
     * The batch will use the current magic.
     * @param offset baseOffset of the single batch and offset of the single record within it
     * @param key
     * @param value
     * @param headers
     * @return The record batch
     */
    public static MutableRecordBatch singleElementRecordBatch(long offset, String key, String value, Header[] headers) {
        return singleElementMemoryRecords(
                DEFAULT_MAGIC_VALUE,
                offset,
                DEFAULT_TIMESTAMP,
                key,
                value,
                headers
        )
         .batches()
         .iterator()
         .next();
    }

    /**
     * Return a singleton RecordBatch containing a single Record with the given key, value and headers.
     * The batch will use the current magic.
     *
     * @param baseOffset baseOffset of the single batch and offset of the single record within it
     * @param compression
     * @return The record batch
     */
    public static MutableRecordBatch singleElementRecordBatch(
            byte magic,
            long baseOffset,
            Compression compression,
            TimestampType timestampType,
            long logAppendTime,
            long producerId,
            short producerEpoch,
            int baseSequence,
            boolean isTransactional,
            boolean isControlBatch,
            int partitionLeaderEpoch,
            byte[] key,
            byte[] value,
            Header... headers
    ) {
        MemoryRecords records = memoryRecordsWithoutCopy(
                magic,
                baseOffset,
                compression,
                timestampType,
                logAppendTime,
                producerId,
                producerEpoch,
                baseSequence,
                isTransactional,
                isControlBatch,
                partitionLeaderEpoch,
                0L,
                key,
                value,
                headers
        );
        return records.batches().iterator().next();
    }

    /**
     * Return a singleton RecordBatch with all records removed. This simulates the case where compaction removes all
     * records but retains the batch metadata. The batch will use the current magic.
     * @param offset baseOffset of the single batch and offset of the single record within it
     * @return The batch
     * @see RecordTestUtils#memoryRecordsWithAllRecordsRemoved(long)
     */
    public static MutableRecordBatch recordBatchWithAllRecordsRemoved(long offset) {
        return memoryRecordsWithAllRecordsRemoved(offset).batchIterator().next();
    }

    /**
     * Return a MemoryRecords containing a single RecordBatch containing a single Record with the given key, value and headers.
     * The batch will use the current magic.
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static MemoryRecords singleElementMemoryRecords(String key, String value, Header... headers) {
        return singleElementMemoryRecords(
                DEFAULT_MAGIC_VALUE,
                DEFAULT_OFFSET,
                DEFAULT_TIMESTAMP,
                key,
                value,
                headers
        );
    }

    /**
     * Return a MemoryRecords containing a single RecordBatch containing a single Record with the given key, value and headers.
     * The batch will use the current magic.
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static MemoryRecords singleElementMemoryRecords(byte[] key, byte[] value, Header... headers) {
        return singleElementMemoryRecords(
                DEFAULT_MAGIC_VALUE,
                DEFAULT_OFFSET,
                DEFAULT_TIMESTAMP,
                key,
                value,
                headers
        );
    }

    /**
     * Return a MemoryRecords containing the specified batches
     */
    public static MemoryRecords memoryRecords(MutableRecordBatch... batches) {
        try (ByteBufferOutputStream outputStream = new ByteBufferOutputStream(1000)) {
            for (MutableRecordBatch batch : batches) {
                batch.writeTo(outputStream);
            }
            ByteBuffer buffer = outputStream.buffer();
            buffer.flip();
            return MemoryRecords.readableRecords(buffer);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Return a MemoryRecords containing a single RecordBatch containing a single Record with the given key, value and headers.
     * The batch will use the current magic.
     * @param magic
     * @param offset
     * @param timestamp
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static MemoryRecords singleElementMemoryRecords(byte magic, long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header... headers) {
        return memoryRecordsWithoutCopy(magic, offset, timestamp, bytesOf(key), bytesOf(value), headers);
    }

    /**
     * Return a MemoryRecords containing a single RecordBatch containing a single Record with the given key, value and headers.
     * The batch will use the current magic.
     * @param magic
     * @param offset
     * @param timestamp
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static MemoryRecords singleElementMemoryRecords(byte magic, long offset, long timestamp, String key, String value, Header... headers) {
        byte[] keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        return memoryRecordsWithoutCopy(magic, offset, timestamp, keyBytes, valueBytes, headers);
    }

    /**
     * Return a MemoryRecords containing a single RecordBatch containing a single Record with the given key, value and headers.
     * The batch will use the current magic.
     * @param magic
     * @param offset
     * @param timestamp
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static MemoryRecords singleElementMemoryRecords(byte magic, long offset, long timestamp, byte[] key, byte[] value, Header... headers) {
        // No need to copy the arrays because their contents are written to a ByteBuffer and not retained
        return memoryRecordsWithoutCopy(magic, offset, timestamp, key, value, headers);
    }

    private static MemoryRecords memoryRecordsWithoutCopy(byte magic, long offset, long timestamp, byte[] key, byte[] value, Header... headers) {
        try (MemoryRecordsBuilder memoryRecordsBuilder = defaultMemoryRecordsBuilder(magic)) {
            memoryRecordsBuilder.appendWithOffset(offset, timestamp, key, value, headers);
            return memoryRecordsBuilder.build();
        }
    }

    private static MemoryRecords memoryRecordsWithoutCopy(
            byte magic,
            long baseOffset,
            Compression compression,
            TimestampType timestampType,
            long logAppendTime,
            long producerId,
            short producerEpoch,
            int baseSequence,
            boolean isTransactional,
            boolean isControlBatch,
            int partitionLeaderEpoch,
            long timestamp,
            byte[] key,
            byte[] value,
            Header... headers
    ) {
        try (MemoryRecordsBuilder memoryRecordsBuilder = memoryRecordsBuilder(
                magic,
                baseOffset,
                compression,
                timestampType,
                logAppendTime,
                producerId,
                producerEpoch,
                baseSequence,
                isTransactional,
                isControlBatch,
                partitionLeaderEpoch
        )) {
            memoryRecordsBuilder.appendWithOffset(baseOffset, timestamp, key, value, headers);
            return memoryRecordsBuilder.build();
        }
    }

    /**
     * Return a MemoryRecords containing a single RecordBatch containing multiple Records.
     * The batch will use the current magic.
     * @param records
     * @return The MemoryRecords
     */
    public static MemoryRecords memoryRecords(@NonNull
    List<Record> records) {
        try (MemoryRecordsBuilder memoryRecordsBuilder = defaultMemoryRecordsBuilder(DEFAULT_MAGIC_VALUE)) {
            records.forEach(record -> memoryRecordsBuilder.appendWithOffset(record.offset(), record));
            return memoryRecordsBuilder.build();
        }
    }

    /**
     * Simulates a MemoryRecords that contained some records, but then had all it's records removed by log compaction. Sets
     * the baseOffset of the single batch within the MemoryRecords to 0.
     * @see RecordTestUtils#memoryRecordsWithAllRecordsRemoved(long)
     */
    public static MemoryRecords memoryRecordsWithAllRecordsRemoved() {
        return memoryRecordsWithAllRecordsRemoved(0L);
    }

    /**
     * This is a special case that is different from {@link MemoryRecords#EMPTY}. An empty MemoryRecords is
     * backed by a 0-length buffer. In this case we are simulating a MemoryRecords that contained some
     * records, but then had all it's records removed by log compaction.
     * <p>
     * From the documentation: ... magic v2 and above preserves the first and last offset/sequence numbers
     * from the original batch when the log is cleaned. This is required in order to be able to restore the
     * producer's state when the log is reloaded. ... As a result, it is possible to have empty batches in
     * the log when all the records in the batch are cleaned but batch is still retained in order to preserve
     * a producer's last sequence number.
     * </p>
     * @param baseOffset the baseOffset of the single batch contained in the output MemoryRecords
     * @see <a href="https://kafka.apache.org/documentation/#recordbatch">Apache Kafka RecordBatch documentation</a>
     */
    @NonNull
    public static MemoryRecords memoryRecordsWithAllRecordsRemoved(long baseOffset) {
        try (MemoryRecordsBuilder memoryRecordsBuilder = memoryRecordsBuilder(DEFAULT_MAGIC_VALUE, baseOffset)) {
            // append arbitrary record
            memoryRecordsBuilder.append(DEFAULT_TIMESTAMP, new byte[]{ 1, 2, 3 }, new byte[]{ 1, 2, 3 });
            MemoryRecords records = memoryRecordsBuilder.build();
            ByteBuffer output = ByteBuffer.allocate(1024);
            records.filterTo(new TopicPartition("any", 1), new MemoryRecords.RecordFilter(DEFAULT_TIMESTAMP, 0L) {
                @Override
                protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                    return new BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false);
                }

                @Override
                protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                    return false;
                }
            }, output, 1, BufferSupplier.NO_CACHING);

            output.flip();
            return MemoryRecords.readableRecords(output);
        }
    }

    private static MemoryRecordsBuilder defaultMemoryRecordsBuilder(byte magic) {
        return memoryRecordsBuilder(magic, 0L);
    }

    @NonNull
    private static MemoryRecordsBuilder memoryRecordsBuilder(byte magic, long baseOffset) {
        return memoryRecordsBuilder(
                magic,
                baseOffset,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,
                magic > RecordBatch.MAGIC_VALUE_V1 ? 0L : RecordBatch.NO_PRODUCER_ID,
                (short) 0,
                0,
                false,
                false,
                0
        );
    }

    @NonNull
    private static MemoryRecordsBuilder memoryRecordsBuilder(
            byte magic,
            long baseOffset,
            Compression compression,
            TimestampType timestampType,
            long logAppendTime,
            long producerId,
            short producerEpoch,
            int baseSequence,
            boolean isTransactional,
            boolean isControlBatch,
            int partitionLeaderEpoch
    ) {
        return new MemoryRecordsBuilder(
                ByteBuffer.allocate(1024),
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
                0
        );
    }

    /**
     * Generate a record batch set to be transaction and a control batch containing a single
     * end transaction marker record of type abort
     * @param baseOffset base offset of the batch
     * @return batch
     */
    public static MutableRecordBatch abortTransactionControlBatch(int baseOffset) {
        try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                ByteBuffer.allocate(1000),
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                baseOffset,
                1L,
                1L,
                (short) 1,
                1,
                true,
                true,
                1,
                1
        )) {
            builder.appendEndTxnMarker(1l, new EndTransactionMarker(ControlRecordType.ABORT, 1));
            MemoryRecords controlBatchRecords = builder.build();
            return controlBatchRecords.batchIterator().next();
        }
    }
}
