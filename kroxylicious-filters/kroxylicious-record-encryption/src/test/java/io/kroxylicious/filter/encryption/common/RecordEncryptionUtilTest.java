/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.filter.encryption.common;

import java.nio.ByteBuffer;
import java.util.function.Function;
import java.util.stream.LongStream;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.transform.BatchAwareMemoryRecordsBuilder;
import io.kroxylicious.test.record.RecordTestUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class RecordEncryptionUtilTest {

    private static final byte[] HELLO_PLAIN_WORLD = "Hello World".getBytes(UTF_8);
    private static final int RECORD_COUNT = 10;

    @Test
    void shouldCountRecordsInBatch() {
        // Given
        final MemoryRecords memoryRecords = makeRecord(-1);

        // When
        final int actual = RecordEncryptionUtil.totalRecordsInBatches(memoryRecords);

        // Then
        assertThat(actual).isEqualTo(RECORD_COUNT);
    }

    @Test
    void shouldCountRecordsInMultipleBatches() {
        // Given
        final MemoryRecords memoryRecords = makeRecord(5);

        // When
        final int actual = RecordEncryptionUtil.totalRecordsInBatches(memoryRecords);

        // Then
        assertThat(actual).isEqualTo(RECORD_COUNT);
    }

    private static MemoryRecords makeRecord(int batchOffset) {
        return makeRecords(
                LongStream.range(0, RecordEncryptionUtilTest.RECORD_COUNT),
                u -> RecordTestUtils.record(ByteBuffer.wrap(RecordEncryptionUtilTest.HELLO_PLAIN_WORLD), new RecordHeader("myKey", "myValue".getBytes())),
                batchOffset
        );
    }

    private static MemoryRecords makeRecords(LongStream offsets, Function<Long, Record> messageFunc, int batchOffset) {
        var stream = new ByteBufferOutputStream(ByteBuffer.allocate(1000));
        var recordsBuilder = memoryRecordsBuilderForStream(stream);
        newBatch(recordsBuilder);
        offsets.forEach(offset -> {
            if (offset == batchOffset) {
                newBatch(recordsBuilder);
            }
            recordsBuilder.appendWithOffset(offset, messageFunc.apply(offset));
        });

        return recordsBuilder.build();
    }

    private static void newBatch(BatchAwareMemoryRecordsBuilder recordsBuilder) {
        recordsBuilder.addBatch(
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,
                RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                RecordBatch.NO_TIMESTAMP
        );
    }

    @NonNull
    private static BatchAwareMemoryRecordsBuilder memoryRecordsBuilderForStream(ByteBufferOutputStream stream) {
        return new BatchAwareMemoryRecordsBuilder(stream);
    }
}
