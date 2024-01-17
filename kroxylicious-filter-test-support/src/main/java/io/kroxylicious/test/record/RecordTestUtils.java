/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

/**
 * Utilities for easily creating Records, MemoryRecords etc, for use in tests
 */
public class RecordTestUtils {

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
        return record(RecordBatch.CURRENT_MAGIC_VALUE, 0, 0, null, value, headers);
    }

    /**
     * Return a Record with the given value and headers
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(ByteBuffer value,
                                Header... headers) {
        return record(RecordBatch.CURRENT_MAGIC_VALUE, 0, 0, null, value, headers);
    }

    /**
     * Return a Record with the given value and headers
     * @param value
     * @param headers
     * @return The record
     */
    public static Record record(String value,
                                Header... headers) {
        return record(RecordBatch.CURRENT_MAGIC_VALUE,
                0,
                0,
                null,
                value,
                headers);
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
        return record(RecordBatch.CURRENT_MAGIC_VALUE, 0, 0, key, value, headers);
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
        return record(RecordBatch.CURRENT_MAGIC_VALUE,
                0,
                0,
                key,
                value,
                headers);
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
        return record(RecordBatch.CURRENT_MAGIC_VALUE, 0, 0, key, value, headers);
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
        return memoryRecords(RecordBatch.CURRENT_MAGIC_VALUE,
                0,
                0,
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
        return memoryRecords(RecordBatch.CURRENT_MAGIC_VALUE,
                0,
                0,
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
        try (MemoryRecordsBuilder memoryRecordsBuilder = new MemoryRecordsBuilder(
                ByteBuffer.allocate(1024),
                magic,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0L,
                0L,
                0L,
                (short) 0,
                0,
                false,
                false,
                0,
                0)) {
            memoryRecordsBuilder.appendWithOffset(offset, timestamp, key, value, headers);
            return memoryRecordsBuilder.build();
        }
    }
}
