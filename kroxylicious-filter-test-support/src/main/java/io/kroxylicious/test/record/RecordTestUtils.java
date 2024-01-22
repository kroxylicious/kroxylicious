/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.BufferSupplier;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Utilities for easily creating Records, MemoryRecords etc, for use in tests
 */
public class RecordTestUtils {

    private static final byte DEFAULT_MAGIC_VALUE = RecordBatch.CURRENT_MAGIC_VALUE;
    private static final long DEFAULT_OFFSET = 0;
    private static final long DEFAULT_TIMESTAMP = 0;
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
        return record.value() == null ? null : new String(bytesOf(record.value()), StandardCharsets.UTF_8);
    }

    public static byte[] recordKeyAsBytes(Record record) {
        return bytesOf(record.key());
    }

    public static String recordKeyAsString(Record record) {
        return record.key() == null ? null : new String(bytesOf(record.key()), StandardCharsets.UTF_8);
    }

    /**
     * Return a record with the given value and header.
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(byte[] value,
                                Header... headers) {
        return record(DEFAULT_MAGIC_VALUE, DEFAULT_OFFSET, DEFAULT_TIMESTAMP, DEFAULT_KEY_BYTES, value, headers);
    }

    /**
     * Return a Record with the given value and headers
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(ByteBuffer value,
                                Header... headers) {
        return record(DEFAULT_MAGIC_VALUE, DEFAULT_OFFSET, DEFAULT_TIMESTAMP, DEFAULT_KEY_BUFFER, value, headers);
    }

    /**
     * Return a Record with the given value, offset and headers
     * @param offset
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(long offset,
                                ByteBuffer value,
                                Header... headers) {
        return record(DEFAULT_MAGIC_VALUE, offset, DEFAULT_TIMESTAMP, DEFAULT_KEY_BUFFER, value, headers);
    }

    /**
     * Return a Record with the given value and headers
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(String value,
                                Header... headers) {
        return record(DEFAULT_MAGIC_VALUE, DEFAULT_OFFSET, DEFAULT_TIMESTAMP, DEFAULT_KEY_STRING, value, headers);
    }

    /**
     * Return a Record with the given key, value and headers
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(byte[] key,
                                byte[] value,
                                Header... headers) {
        return record(DEFAULT_MAGIC_VALUE, DEFAULT_OFFSET, DEFAULT_TIMESTAMP, key, value, headers);
    }

    /**
     * Return a Record with the given key, value and headers
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(String key,
                                String value,
                                Header... headers) {
        return record(DEFAULT_MAGIC_VALUE, DEFAULT_OFFSET, DEFAULT_TIMESTAMP, key, value, headers);
    }

    /**
     * Return a Record with the given key, value and headers
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(ByteBuffer key,
                                ByteBuffer value,
                                Header... headers) {
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
    public static Record record(byte magic,
                                long offset,
                                long timestamp,
                                byte[] key,
                                byte[] value,
                                Header... headers) {
        // This is a bit of a rigmarole, but it ensures that calls to getSizeInBytes()
        // on the returned Record is actually correct
        MemoryRecords mr = memoryRecords(magic, offset, timestamp, key, value, headers);
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
    public static Record record(byte magic,
                                long offset,
                                long timestamp,
                                String key,
                                String value,
                                Header... headers) {
        // This is a bit of a rigmarole, but it ensures that calls to getSizeInBytes()
        // on the returned Record is actually correct
        MemoryRecords mr = memoryRecords(magic, offset, timestamp, key, value, headers);
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
    public static Record record(byte magic,
                                long offset,
                                long timestamp,
                                ByteBuffer key,
                                ByteBuffer value,
                                Header... headers) {
        // This is a bit of a rigmarole, but it ensures that calls to getSizeInBytes()
        // on the returned Record is actually correct
        MemoryRecords mr = memoryRecords(magic, offset, timestamp, key, value, headers);
        return MemoryRecords.readableRecords(mr.buffer()).records().iterator().next();
    }

    /**
     * Return a singleton RecordBatch containing a single Record with the given key, value and headers.
     * The batch will use the current magic.
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static RecordBatch recordBatch(String key,
                                          String value,
                                          Header... headers) {
        return memoryRecords(DEFAULT_MAGIC_VALUE,
                DEFAULT_OFFSET,
                DEFAULT_TIMESTAMP,
                key,
                value,
                headers)
                .firstBatch();
    }

    /**
     * Return a MemoryRecords containing a single RecordBatch containing a single Record with the given key, value and headers.
     * The batch will use the current magic.
     * @param key
     * @param value
     * @param headers
     * @return The record
     */
    public static MemoryRecords memoryRecords(String key, String value, Header... headers) {
        return memoryRecords(DEFAULT_MAGIC_VALUE,
                DEFAULT_OFFSET,
                DEFAULT_TIMESTAMP,
                key,
                value,
                headers);
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
    public static MemoryRecords memoryRecords(byte magic, long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header... headers) {
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
    public static MemoryRecords memoryRecords(byte magic, long offset, long timestamp, String key, String value, Header... headers) {
        return memoryRecordsWithoutCopy(magic, offset, timestamp, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), headers);
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
    public static MemoryRecords memoryRecords(byte magic, long offset, long timestamp, byte[] key, byte[] value, Header... headers) {
        // No need to copy the arrays because their contents are written to a ByteBuffer and not retained
        return memoryRecordsWithoutCopy(magic, offset, timestamp, key, value, headers);
    }

    private static MemoryRecords memoryRecordsWithoutCopy(byte magic, long offset, long timestamp, byte[] key, byte[] value, Header... headers) {
        try (MemoryRecordsBuilder memoryRecordsBuilder = defaultMemoryRecordsBuilder(magic)) {
            memoryRecordsBuilder.appendWithOffset(offset, timestamp, key, value, headers);
            return memoryRecordsBuilder.build();
        }
    }

    /**
     * Return a MemoryRecords containing a single RecordBatch containing multiple Records.
     * The batch will use the current magic.
     * @param records
     * @return The MemoryRecords
     */
    public static MemoryRecords memoryRecords(@NonNull List<Record> records) {
        try (MemoryRecordsBuilder memoryRecordsBuilder = defaultMemoryRecordsBuilder(DEFAULT_MAGIC_VALUE)) {
            records.forEach(record -> memoryRecordsBuilder.appendWithOffset(record.offset(), record));
            return memoryRecordsBuilder.build();
        }
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
     * @see <a href="https://kafka.apache.org/documentation/#recordbatch">Apache Kafka RecordBatch documentation</a>
     */
    public static MemoryRecords memoryRecordsWithAllRecordsRemoved() {
        return memoryRecordsWithAllRecordsRemoved(0L);
    }

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
        return new MemoryRecordsBuilder(
                ByteBuffer.allocate(1024),
                magic,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                baseOffset,
                0L,
                0L,
                (short) 0,
                0,
                false,
                false,
                0,
                0);
    }
}
