/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.records;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchAwareMemoryRecordsBuilderTest {

    // Can't append a record without a batch
    @Test
    void shouldRequireABatchBeforeAppend() {
        // Given
        var builder = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(100));

        // Then
        assertThatThrownBy(() -> builder.append((Record) null))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("You must start a batch");
    }

    // 0 batches
    @Test
    void shouldAllowNoBatches() {
        // Given
        var builder = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(100));

        // When
        var mr = builder.build();

        // Then
        assertThat(StreamSupport.stream(mr.batches().spliterator(), false).count())
                .isZero();
        assertThat(StreamSupport.stream(mr.records().spliterator(), false).count())
                .isZero();
    }

    // Single batch of 0 records
    @Test
    void shouldAllowEmptyBatches() {
        // Given
        var builder = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(100));
        builder.addBatch(RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0,
                0,
                0,
                (short) 0,
                0,
                false,
                false,
                0,
                0);

        // When
        var mr = builder.build();

        // Then
        assertThat(StreamSupport.stream(mr.batches().spliterator(), false).count())
                .isZero();
        assertThat(StreamSupport.stream(mr.records().spliterator(), false).count())
                .isZero();
    }

    // Single batch of 1 record
    @Test
    void shouldSupportNonEmptyBatch() {
        // Given
        var builder = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(100));
        builder.addBatch(RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0,
                0,
                0,
                (short) 0,
                0,
                false,
                false,
                0,
                0);
        builder.append(new SimpleRecord("hello".getBytes(StandardCharsets.UTF_8)));

        // When
        var mr = builder.build();

        // Then
        assertThat(StreamSupport.stream(mr.batches().spliterator(), false).count())
                .isEqualTo(1);
        assertThat(StreamSupport.stream(mr.records().spliterator(), false).count())
                .isEqualTo(1);
    }

    // >1 batches
    @ParameterizedTest
    @ValueSource(ints = { 1, 1000 })
    void shouldSupportMultipleBatches(int initialBufferSize) {
        // Given
        var builder = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(initialBufferSize));
        builder.addBatch(RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0,
                0,
                0,
                (short) 0,
                0,
                false,
                false,
                0,
                0);
        builder.append(new SimpleRecord("hello".getBytes(StandardCharsets.UTF_8)));
        builder.addBatch(RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.ZSTD,
                TimestampType.LOG_APPEND_TIME,
                1, // not base off
                0,
                0,
                (short) 0,
                0,
                false,
                false,
                0,
                0);
        builder.append(new SimpleRecord("hello2".getBytes(StandardCharsets.UTF_8)));

        // When
        var mr = builder.build();

        // Then
        List<MutableRecordBatch> batches = StreamSupport.stream(mr.batches().spliterator(), false).toList();
        assertThat(batches).hasSize(2);

        var batch1 = batches.get(0);
        assertThat(batch1.compressionType()).isEqualTo(CompressionType.NONE);
        assertThat(batch1.iterator().next().value()).isEqualTo(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
        assertThat(batch1.iterator().next().offset()).isZero();

        var batch2 = batches.get(1);
        assertThat(batch2.compressionType()).isEqualTo(CompressionType.ZSTD);
        assertThat(batch2.iterator().next().value()).isEqualTo(ByteBuffer.wrap("hello2".getBytes(StandardCharsets.UTF_8)));
        assertThat(batch2.iterator().next().offset()).isEqualTo(1);

    }

    // Control batches are propagated
    @ParameterizedTest
    @ValueSource(ints = { 1, 1000 })
    void shouldSupportControlBatches(int initialBufferSize) {
        // Given
        var builder = new BatchAwareMemoryRecordsBuilder(new ByteBufferOutputStream(initialBufferSize));
        builder.addBatch(RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0,
                0,
                0,
                (short) 0,
                0,
                false,
                false,
                0,
                0);
        builder.append(new SimpleRecord("data-key".getBytes(StandardCharsets.UTF_8), "data-value".getBytes(StandardCharsets.UTF_8)));
        builder.addBatch(RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.ZSTD,
                TimestampType.LOG_APPEND_TIME,
                1,
                0,
                0,
                (short) 0,
                0,
                false,
                true,
                0,
                0);
        SimpleRecord controlRecord = controlRecord();
        builder.appendControlRecordWithOffset(1, controlRecord);

        // When
        var mr = builder.build();

        // Then
        List<MutableRecordBatch> batches = StreamSupport.stream(mr.batches().spliterator(), false).toList();
        assertThat(batches).hasSize(2);

        var batch1 = batches.get(0);
        assertThat(batch1.compressionType()).isEqualTo(CompressionType.NONE);
        assertThat(batch1.iterator().next().value()).isEqualTo(ByteBuffer.wrap("data-value".getBytes(StandardCharsets.UTF_8)));

        var batch2 = batches.get(1);
        assertThat(batch2.compressionType()).isEqualTo(CompressionType.ZSTD);
        assertThat(batch2.iterator().next().value()).isEqualTo(controlRecord.value());
    }

    @NonNull
    private static SimpleRecord controlRecord() {
        var key = ControlRecordType.ABORT.recordKey();
        var bb = ByteBuffer.allocate(key.sizeOf());
        key.writeTo(bb);
        SimpleRecord controlRecord = new SimpleRecord(bb.array(), "control-value".getBytes(StandardCharsets.UTF_8));
        return controlRecord;
    }

    // we can reuse the ByteBufferOutputStream between instantiations of the builder
    @Test
    void shouldSupportBufferReuse() {

        // Given
        ByteBufferOutputStream buffer = new ByteBufferOutputStream(1);

        // When
        var builder1 = new BatchAwareMemoryRecordsBuilder(buffer);
        builder1.addBatch(RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0,
                0,
                0,
                (short) 0,
                0,
                false,
                false,
                0,
                0)
                .append(new SimpleRecord("hello1".getBytes(StandardCharsets.UTF_8)));
        var mr1 = builder1.build();

        ByteBuffer bb1 = buffer.buffer();
        assertThat(bb1.position()).isZero();
        assertThat(bb1.capacity()).isEqualTo(80);

        var builder2 = new BatchAwareMemoryRecordsBuilder(buffer);
        builder2.addBatch(RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0,
                0,
                0,
                (short) 0,
                0,
                false,
                false,
                0,
                0)
                .append(new SimpleRecord("hello2".getBytes(StandardCharsets.UTF_8)));
        var mr2 = builder2.build();

        // Then
        ByteBuffer bb2 = buffer.buffer();
        assertThat(bb1).isSameAs(bb2);
        assertThat(bb1.position()).isZero();
        assertThat(bb1.capacity()).isEqualTo(80);

        assertThat(StreamSupport.stream(mr1.batches().spliterator(), false).count())
                .isEqualTo(1);
        assertThat(StreamSupport.stream(mr1.records().spliterator(), false).count())
                .isEqualTo(1);
        assertThat(StreamSupport.stream(mr2.batches().spliterator(), false).count())
                .isEqualTo(1);
        assertThat(StreamSupport.stream(mr2.records().spliterator(), false).count())
                .isEqualTo(1);
    }

}
