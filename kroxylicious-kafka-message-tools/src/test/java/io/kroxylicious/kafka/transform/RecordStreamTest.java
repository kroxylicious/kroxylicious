/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;

import io.kroxylicious.test.assertj.MemoryRecordsAssert;
import io.kroxylicious.test.assertj.RecordBatchAssert;
import io.kroxylicious.test.record.RecordTestUtils;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.test.assertj.KafkaAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordStreamTest {

    @Test
    void ofRecordsShouldRejectNull() {
        assertThatThrownBy(() -> RecordStream.ofRecords(null)).isExactlyInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> RecordStream.ofRecordsWithIndex(null)).isExactlyInstanceOf(NullPointerException.class);
    }

    @Test
    void ofRecordsToList() {
        var mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(ByteBuffer.allocate(10)));
        var mr = mrb.addBatch(Compression.NONE, TimestampType.LOG_APPEND_TIME, 10)
                    .append(new SimpleRecord(42, "hello".getBytes(StandardCharsets.UTF_8), "world".getBytes(StandardCharsets.UTF_8), new Header[0]))
                    .build();
        RecordStream<Void> rs = RecordStream.ofRecords(mr);
        assertThat(rs).isNotNull();
        var list = rs.mapConstant("prefix")
                     .toList(
                             (batch, record, state) -> state
                                                       + StandardCharsets.UTF_8.decode(record.key())
                                                       + StandardCharsets.UTF_8.decode(record.value())
                     );
        assertThat(list).singleElement().isEqualTo("prefixhelloworld");
    }

    @Test
    void toMemoryRecordsPreservesControlBatch() {
        MutableRecordBatch records = RecordTestUtils.abortTransactionControlBatch(0);
        Record controlRecord = records.iterator().next();
        MemoryRecords memoryRecords = RecordTestUtils.memoryRecords(records);
        RecordStream<Void> rs = RecordStream.ofRecords(memoryRecords);
        MemoryRecords output = rs.toMemoryRecords(new ByteBufferOutputStream(1024), new Prefixer<>());
        MemoryRecordsAssert.assertThat(output)
                           .hasNumBatches(1)
                           .firstBatch()
                           .isControlBatch(true)
                           .hasNumRecords(1)
                           .firstRecord()
                           .isEqualTo(controlRecord);
    }

    @Test
    void toMemoryRecordsPreservesEmptyBatch() {
        MutableRecordBatch records = RecordTestUtils.recordBatchWithAllRecordsRemoved(0);
        MemoryRecords memoryRecords = RecordTestUtils.memoryRecords(records);
        RecordStream<Void> rs = RecordStream.ofRecords(memoryRecords);
        MemoryRecords output = rs.toMemoryRecords(new ByteBufferOutputStream(1024), new Prefixer<>());
        MemoryRecordsAssert.assertThat(output)
                           .hasNumBatches(1)
                           .firstBatch()
                           .hasNumRecords(0);
    }

    @Test
    void ofRecordsToSet() {
        var mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(ByteBuffer.allocate(10)));
        var mr = mrb.addBatch(Compression.NONE, TimestampType.LOG_APPEND_TIME, 10)
                    .append(new SimpleRecord(42, "hello".getBytes(StandardCharsets.UTF_8), "world".getBytes(StandardCharsets.UTF_8), new Header[0]))
                    .build();
        RecordStream<Void> rs = RecordStream.ofRecords(mr);
        assertThat(rs).isNotNull();
        var set = rs.mapConstant("prefix")
                    .toSet(
                            (batch, record, state) -> state
                                                      + StandardCharsets.UTF_8.decode(record.key())
                                                      + StandardCharsets.UTF_8.decode(record.value())
                    );
        assertThat(set).singleElement().isEqualTo("prefixhelloworld");
    }

    @Test
    void ofRecordsForEachRecord() {
        var mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(ByteBuffer.allocate(10)));
        var mr = mrb.addBatch(Compression.NONE, TimestampType.CREATE_TIME, 10)
                    .append(new SimpleRecord(42, "hello".getBytes(StandardCharsets.UTF_8), "world".getBytes(StandardCharsets.UTF_8), new Header[0]))
                    .build();
        RecordStream<Void> rs = RecordStream.ofRecords(mr);
        assertThat(rs).isNotNull();
        rs.mapConstant("prefix").forEachRecord((batch, record, state) -> {
            assertThat(state).isEqualTo("prefix");
            assertThat(record).hasTimestampEqualTo(42L)
                              .hasKeyEqualTo("hello")
                              .hasValueEqualTo("world")
                              .hasOffsetEqualTo(10);
        });
    }

    @Test
    void ofRecordsToMemoryRecords() {
        var mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(ByteBuffer.allocate(10)));
        int baseOffset = 11;
        var mr = mrb.addBatch(Compression.NONE, TimestampType.CREATE_TIME, baseOffset)
                    .append(new SimpleRecord(42, "hello".getBytes(StandardCharsets.UTF_8), "world".getBytes(StandardCharsets.UTF_8), new Header[0]))
                    .build();
        RecordStream<Void> rs = RecordStream.ofRecords(mr);
        assertThat(rs).isNotNull();
        var records = rs.mapConstant("prefix")
                        .toMemoryRecords(
                                new ByteBufferOutputStream(ByteBuffer.allocate(10)),
                                new Prefixer<>()
                        );
        assertThat(records)
                           .hasNumBatches(1)
                           .firstBatch()
                           .firstRecord()
                           .hasKeyEqualTo("prefixhello")
                           .hasValueEqualTo("prefixworld")
                           .hasOffsetEqualTo(Math.abs("prefix".hashCode()) + baseOffset)
                           .hasTimestampEqualTo(Math.abs("prefix".hashCode()) + 42L);
    }

    @Test
    void ofRecordsWithIndexToMemoryRecords() {
        var mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(ByteBuffer.allocate(10)));
        int baseOffset = 10;
        var mr = mrb.addBatch(Compression.NONE, TimestampType.CREATE_TIME, baseOffset)
                    .append(new SimpleRecord(42, "hello".getBytes(StandardCharsets.UTF_8), "world".getBytes(StandardCharsets.UTF_8), new Header[0]))
                    .append(new SimpleRecord(65, "HELLO".getBytes(StandardCharsets.UTF_8), "WORLD".getBytes(StandardCharsets.UTF_8), new Header[0]))
                    .build();
        RecordStream<Integer> rs = RecordStream.ofRecordsWithIndex(mr);
        assertThat(rs).isNotNull();
        var records = rs.toMemoryRecords(
                new ByteBufferOutputStream(ByteBuffer.allocate(10)),
                new Prefixer<>()
        );
        RecordBatchAssert batch = assertThat(records)
                                                     .hasNumBatches(1)
                                                     .firstBatch();
        var index = 0;
        batch.firstRecord()
             .hasKeyEqualTo(index + "hello")
             .hasValueEqualTo(index + "world")
             .hasOffsetEqualTo(Math.abs(Integer.hashCode(index)) + baseOffset)
             .hasTimestampEqualTo(Math.abs(Integer.hashCode(index)) + 42L);
        index++;
        batch.lastRecord()
             .hasKeyEqualTo(index + "HELLO")
             .hasValueEqualTo(index + "WORLD")
             .hasOffsetEqualTo(Math.abs(Integer.hashCode(index)) + baseOffset + 1)
             .hasTimestampEqualTo(Math.abs(Integer.hashCode(index)) + 65L);
    }

    @Test
    void ofRecordsMapPerRecordToMemoryRecords() {
        var mrb = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(ByteBuffer.allocate(10)));
        int baseOffset = 10;
        var mr = mrb.addBatch(Compression.NONE, TimestampType.CREATE_TIME, baseOffset)
                    .append(new SimpleRecord(42, "hello".getBytes(StandardCharsets.UTF_8), "world".getBytes(StandardCharsets.UTF_8), new Header[0]))
                    .append(new SimpleRecord(65, "HELLO".getBytes(StandardCharsets.UTF_8), "WORLD".getBytes(StandardCharsets.UTF_8), new Header[0]))
                    .build();
        RecordStream<Integer> rs = RecordStream.ofRecordsWithIndex(mr);
        assertThat(rs).isNotNull();
        var records = rs.mapPerRecord((batch, record, index) -> index + 14)
                        .toMemoryRecords(
                                new ByteBufferOutputStream(ByteBuffer.allocate(10)),
                                new Prefixer<>()
                        );
        RecordBatchAssert batch = assertThat(records)
                                                     .hasNumBatches(1)
                                                     .firstBatch();
        var index = 14;
        batch.firstRecord()
             .hasKeyEqualTo(index + "hello")
             .hasValueEqualTo(index + "world")
             .hasOffsetEqualTo(Math.abs(Integer.hashCode(index)) + baseOffset)
             .hasTimestampEqualTo(Math.abs(Integer.hashCode(index)) + 42L);
        index++;
        batch.lastRecord()
             .hasKeyEqualTo(index + "HELLO")
             .hasValueEqualTo(index + "WORLD")
             .hasOffsetEqualTo(Math.abs(Integer.hashCode(index)) + baseOffset + 1)
             .hasTimestampEqualTo(Math.abs(Integer.hashCode(index)) + 65L);
    }

    @NonNull
    private static ByteBuffer prefix(@NonNull
    String prefix, @NonNull
    ByteBuffer buffer) {
        return StandardCharsets.UTF_8.encode(CharBuffer.wrap(prefix + StandardCharsets.UTF_8.decode(buffer)));
    }

    private static class Prefixer<T> implements RecordTransform<T> {
        private T state;

        @Override
        public void initBatch(@NonNull
        RecordBatch batch) {

        }

        @Override
        public void init(
                T state,
                @NonNull
                Record record
        ) {
            this.state = state;
        }

        @Override
        public void resetAfterTransform(
                T state,
                @NonNull
                Record record
        ) {

        }

        @Override
        public long transformOffset(@NonNull
        Record record) {
            return Math.abs(state.hashCode()) + record.offset();
        }

        @Override
        public long transformTimestamp(@NonNull
        Record record) {
            return Math.abs(state.hashCode()) + record.timestamp();
        }

        @Nullable
        @Override
        public ByteBuffer transformKey(@NonNull
        Record record) {
            return prefix(String.valueOf(state), record.key());
        }

        @Nullable
        @Override
        public ByteBuffer transformValue(@NonNull
        Record record) {
            return prefix(String.valueOf(state), record.value());
        }

        @Nullable
        @Override
        public Header[] transformHeaders(@NonNull
        Record record) {
            return new Header[0];
        }
    }
}
