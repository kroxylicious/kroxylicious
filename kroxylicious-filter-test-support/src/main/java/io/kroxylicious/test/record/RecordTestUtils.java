/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

public class RecordTestUtils {

    private RecordTestUtils() {
    }

    /**
     * Create and return a ByteBuffer whose contents is the same as the contents of the given {@code src}
     * between {@code src}'s position and {@code src}'s limit, leaving {@code src.position()} unchanged.
     * @param src The buffer to copy
     * @return A new ByteBuffer with the same contents as {@code src}, or null if {@code src} was null.
     */
    private static ByteBuffer copy(ByteBuffer src) {
        if (src == null) {
            return null;
        }
        var b = ByteBuffer.allocate(src.remaining());
        var p = src.position();
        b.put(src);
        src.position(p);
        return b.flip().asReadOnlyBuffer();
    }

    /**
     * @param src The bytes to copy
     * @return A copy of the bytes in {@code src}, or null if {@code src} was null.
     */
    private static byte[] copy(byte[] src) {
        return src == null ? null : src.clone();
    }

    /**
     * Return a copy of {@code src}, where each element's {@link Header#value()} is also copied.
     * @param src The headers to copy.
     * @return The copy.
     */
    private static Header[] copy(Header[] src) {
        if (src == null) {
            return null;
        }
        var result = new Header[src.length];
        for (int i = 0; i < src.length; i++) {
            Header header = src[i];
            result[i] = new RecordHeader(header.key(), copy(header.value()));

        }
        return result;
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

    public static byte[] recordValueAsBytes(Record record) {
        return bytesOf(record.value());
    }

    public static Record record(byte[] value,
                                Header... headers) {
        return record(RecordBatch.CURRENT_MAGIC_VALUE, 0, 0, null, value, headers);
    }

    public static Record record(ByteBuffer value,
                                Header... headers) {
        return record(RecordBatch.CURRENT_MAGIC_VALUE, 0, 0, null, value, headers);
    }

    public static Record record(String value,
                                Header... headers) {
        return record(RecordBatch.CURRENT_MAGIC_VALUE,
                0,
                0,
                null,
                value,
                headers);
    }

    public static Record record(byte[] key,
                                byte[] value,
                                Header... headers) {
        return record(RecordBatch.CURRENT_MAGIC_VALUE, 0, 0, key, value, headers);
    }

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

    public static Record record(ByteBuffer key,
                                ByteBuffer value,
                                Header... headers) {
        return record(RecordBatch.CURRENT_MAGIC_VALUE, 0, 0, key, value, headers);
    }

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

    public static MemoryRecords memoryRecords(String key, String value, Header... headers) {
        return memoryRecords(RecordBatch.CURRENT_MAGIC_VALUE,
                0,
                0,
                key,
                value,
                headers);
    }

    public static MemoryRecords memoryRecords(byte magic, long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header... headers) {
        return memoryRecordsWithoutCopy(magic, offset, timestamp, bytesOf(key), bytesOf(value), copy(headers));
    }

    public static MemoryRecords memoryRecords(byte magic, long offset, long timestamp, String key, String value, Header... headers) {
        return memoryRecordsWithoutCopy(magic, offset, timestamp, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), copy(headers));
    }

    public static MemoryRecords memoryRecords(byte magic, long offset, long timestamp, byte[] key, byte[] value, Header... headers) {
        return memoryRecordsWithoutCopy(magic, offset, timestamp, copy(key), copy(value), copy(headers));
    }

    private static MemoryRecords memoryRecordsWithoutCopy(byte magic, long offset, long timestamp, byte[] key, byte[] value, Header... headers) {
        MemoryRecordsBuilder memoryRecordsBuilder = new MemoryRecordsBuilder(
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
                0);
        memoryRecordsBuilder.appendWithOffset(offset, timestamp, key, value, headers);
        return memoryRecordsBuilder.build();
    }
}
