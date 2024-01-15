/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.records;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.filter.encryption.records.BatchAwareMemoryRecordsBuilder.EMPTY_HEADERS;
import static io.kroxylicious.filter.encryption.records.MemoryRecordsUtils.batchStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MemoryRecordsUtilsTest {

    @Test
    void testBatchStream() {
        // Given
        var mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(1));
        for (long offset = 4L; offset < 1000L; offset++) {
            mrb.addBatch(CompressionType.NONE, TimestampType.LOG_APPEND_TIME, offset);
            mrb.appendWithOffset(offset, 123, "a".getBytes(StandardCharsets.UTF_8), null, EMPTY_HEADERS);
        }
        var batch = mrb.build();

        // When
        Supplier<? extends Stream<? extends RecordBatch>> stream = () -> MemoryRecordsUtils.batchStream(batch);

        // Then
        // Assertions about stream itself
        assertThat(stream.get().isParallel()).isFalse();
        Stream<? extends RecordBatch> s1 = stream.get().sorted();
        assertThatThrownBy(() -> s1.toList()).isExactlyInstanceOf(ClassCastException.class);

        // Assertions about stream contents
        assertThat(stream.get().count()).describedAs("Expect 996 batches").isEqualTo(996);
        assertThat(stream.get().flatMap(b -> RecordBatchUtils.recordStream(b)).map(Record::offset).toList()).isEqualTo(LongStream.range(4L, 1000L).boxed().toList());
        assertThat(stream.get().parallel().flatMap(b -> RecordBatchUtils.recordStream(b)).map(Record::offset).toList())
                .isEqualTo(LongStream.range(4L, 1000L).boxed().toList());
        assertThat(stream.get().distinct().flatMap(b -> RecordBatchUtils.recordStream(b)).map(Record::offset).toList())
                .isEqualTo(LongStream.range(4L, 1000L).boxed().toList());
    }

    @Test
    void testCombineBuilders() {
        var mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(1));
        for (long offset = 4L; offset < 1000L; offset++) {
            mrb.addBatch(CompressionType.NONE, TimestampType.LOG_APPEND_TIME, offset);
            mrb.appendWithOffset(offset, 123, "a".getBytes(StandardCharsets.UTF_8), null, EMPTY_HEADERS);
        }
        var templateBatch = mrb.build().firstBatch();

        // Given
        ByteBufferOutputStream resultBuffer1 = new ByteBufferOutputStream(1000);
        var mrb1 = new BatchAwareMemoryRecordsBuilder(resultBuffer1);
        ByteBufferOutputStream resultBuffer = new ByteBufferOutputStream(1000);
        var mrb2 = new BatchAwareMemoryRecordsBuilder(resultBuffer);
        mrb1.addBatchLike(templateBatch);
        mrb2.addBatchLike(templateBatch);
        for (long offset = 0L; offset < 1000L; offset++) {
            mrb1.appendWithOffset(offset, 123, "a".getBytes(StandardCharsets.UTF_8), null, EMPTY_HEADERS);
            mrb2.appendWithOffset(1000L + offset, 123, "a".getBytes(StandardCharsets.UTF_8), null, EMPTY_HEADERS);
        }

        // When
        var result = MemoryRecordsUtils.combineBuilders(mrb1, mrb2).build();

        // Then
        List<? extends RecordBatch> batches = batchStream(result).toList();
        assertThat(batches).hasSize(2);

        assertThat(offsetsOfBatch(batches.get(0)))
                .isEqualTo(range(0L, 1000L));

        assertThat(offsetsOfBatch(batches.get(1)))
                .isEqualTo(range(1000L, 2000L));
    }

    @NonNull
    private List<Long> range(long startInclusive, final long endExclusive) {
        return LongStream.range(startInclusive, endExclusive).boxed().toList();
    }

    @Test
    void testConcatCollector() {
        var mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(1));
        mrb.addBatch(CompressionType.NONE, TimestampType.CREATE_TIME, 4L);
        for (long offset = 4L; offset < 1000L; offset++) {
            mrb.appendWithOffset(offset, 123, "a".getBytes(StandardCharsets.UTF_8), null, EMPTY_HEADERS);
        }
        var mr1 = mrb.build();

        mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(1));
        mrb.addBatch(CompressionType.NONE, TimestampType.CREATE_TIME, 1000L);
        for (long offset = 1000L; offset < 2000L; offset++) {
            mrb.appendWithOffset(offset, 123, "a".getBytes(StandardCharsets.UTF_8), null, EMPTY_HEADERS);
        }
        var mr2 = mrb.build();

        mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(1));
        mrb.addBatch(CompressionType.NONE, TimestampType.CREATE_TIME, 2000L);
        for (long offset = 2000L; offset < 3000L; offset++) {
            mrb.appendWithOffset(offset, 123, "a".getBytes(StandardCharsets.UTF_8), null, EMPTY_HEADERS);
        }
        var mr3 = mrb.build();

        // When
        MemoryRecords memoryRecords = Stream.of(mr1, mr2, mr3).collect(MemoryRecordsUtils.concatCollector(new ByteBufferOutputStream(100)));

        // Then
        List<? extends RecordBatch> batches = batchStream(memoryRecords).toList();
        assertThat(batches).hasSize(3);
        assertThat(batchStream(memoryRecords).map(RecordBatch::baseOffset).toList())
                .isEqualTo(List.of(4L, 1000L, 2000L));

        assertThat(offsetsOfBatch(batches.get(0)))
                .isEqualTo(range(4, 1000L));

        assertThat(offsetsOfBatch(batches.get(1)))
                .isEqualTo(range(1000L, 2000L));

        assertThat(offsetsOfBatch(batches.get(2)))
                .isEqualTo(range(2000L, 3000L));
    }

    @NonNull
    private List<Long> offsetsOfBatch(RecordBatch batch) {
        return RecordBatchUtils.recordStream(batch).map(Record::offset).toList();
    }
}