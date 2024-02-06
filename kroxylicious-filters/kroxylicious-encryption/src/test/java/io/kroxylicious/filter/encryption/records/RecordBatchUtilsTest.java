/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.records;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.filter.encryption.records.BatchAwareMemoryRecordsBuilder.EMPTY_HEADERS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class RecordBatchUtilsTest {

    @Test
    void testRecordStream() {
        // Given
        var mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(1));
        mrb.addBatch(CompressionType.NONE, TimestampType.LOG_APPEND_TIME, 4L);
        for (long offset = 4L; offset < 1000L; offset++) {
            mrb.appendWithOffset(offset, 123, "a".getBytes(StandardCharsets.UTF_8), null, EMPTY_HEADERS);
        }
        var batch = mrb.build().firstBatch();

        // When
        // Use a Supplier so that assertions can terminate the stream they're asserting on
        // without affecting each other
        Supplier<? extends Stream<? extends Record>> stream = () -> RecordBatchUtils.recordStream(batch);

        // Then
        // Assertions about stream itself
        assertThat(stream.get().isParallel()).isFalse();
        Stream<? extends Record> s1 = stream.get().sorted();
        assertThatThrownBy(() -> s1.toList()).describedAs("Records are not Comparable").isExactlyInstanceOf(ClassCastException.class);

        // Assertions about stream contents
        assertThat(stream.get().count())
                .describedAs("Expect 996 records")
                .isEqualTo(996);
        assertThat(stream.get().map(Record::offset).toList()).isEqualTo(LongStream.range(4L, 1000L).boxed().toList());
        assertThat(stream.get().parallel().map(Record::offset).toList()).isEqualTo(LongStream.range(4L, 1000L).boxed().toList());
        assertThat(stream.get().distinct().map(Record::offset).toList()).isEqualTo(LongStream.range(4L, 1000L).boxed().toList());
    }

    @Test
    void testToMemoryRecords() {
        // Given
        var mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(1));
        mrb.addBatch(CompressionType.NONE, TimestampType.LOG_APPEND_TIME, 4L);
        for (long offset = 4L; offset < 1000L; offset++) {
            mrb.appendWithOffset(offset, 123, "a".getBytes(StandardCharsets.UTF_8), null, EMPTY_HEADERS);
        }
        var batch = mrb.build().firstBatch();

        // When
        MyRecordTransform mapper = new MyRecordTransform();
        var memoryRecords = RecordBatchUtils.toMemoryRecords(batch,
                mapper,
                new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(1000))).build();

        // Then
        assertThat(mapper.initCalls).isEqualTo(996);
        assertThat(mapper.state).isZero();
        assertThat(MemoryRecordsUtils.batchStream(memoryRecords).count()).isEqualTo(1);
        assertThat(RecordBatchUtils.recordStream(memoryRecords.firstBatch()).map(Record::offset).toList())
                .isEqualTo(LongStream.range(1_000_000_004L, 1_000_001_000L).boxed().toList());
    }

    private static class MyRecordTransform implements RecordTransform {
        int state = 0;
        int initCalls = 0;

        @Override
        public void init(@NonNull Record record) {
            state += 1;
            if (state != 1) {
                fail("Expect init and reset to be paired");
            }
            initCalls++;
        }

        @Override
        public long transformOffset(Record record) {
            return 1_000_000_000L + record.offset();
        }

        @Override
        public long transformTimestamp(Record record) {
            return 1_000_000_000L + record.timestamp();
        }

        @Override
        public ByteBuffer transformKey(Record record) {
            return record.key();
        }

        @Override
        public ByteBuffer transformValue(Record record) {
            return record.value();
        }

        @Override
        public Header[] transformHeaders(Record record) {
            return record.headers();
        }

        @Override
        public void resetAfterTransform(Record record) {
            state -= 1;
            if (state != 0) {
                fail("Expect init and reset to be paired");
            }
        }
    }
}
