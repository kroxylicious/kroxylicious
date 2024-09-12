/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;

import io.kroxylicious.test.record.RecordTestUtils;

import static io.kroxylicious.test.assertj.Assertions.throwsAssertionErrorContaining;
import static io.kroxylicious.test.record.RecordTestUtils.singleElementRecordBatch;

class RecordBatchAssertTest {

    @Test
    void testRecordBatchHasSizeInBytes() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasSizeInBytes(76);
        throwsAssertionErrorContaining(() -> batchAssert.hasSizeInBytes(1), "[sizeInBytes]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasSizeInBytes(1));
    }

    @Test
    void testRecordBatchHasBaseOffset() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasBaseOffset(0L);
        throwsAssertionErrorContaining(() -> batchAssert.hasBaseOffset(1L), "[baseOffset]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasBaseOffset(1L));
    }

    @Test
    void testRecordBatchHasBaseSequence() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasBaseSequence(0);
        throwsAssertionErrorContaining(() -> batchAssert.hasBaseSequence(1), "[baseSequence]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasBaseSequence(1));
    }

    @Test
    void testRecordBatchHasCompressionType() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasCompressionType(CompressionType.NONE);
        throwsAssertionErrorContaining(() -> batchAssert.hasCompressionType(CompressionType.GZIP), "[compressionType]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasCompressionType(CompressionType.GZIP));
    }

    @Test
    void testRecordBatchHasMagic() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasMagic(RecordBatch.CURRENT_MAGIC_VALUE);
        throwsAssertionErrorContaining(() -> batchAssert.hasMagic((byte) 1), "[magic]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasMagic((byte) 1));
    }

    @Test
    void testRecordBatchIsControlBatch() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatch controlBatch = RecordTestUtils.abortTransactionControlBatch(1);
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        RecordBatchAssert controlBatchAssert = KafkaAssertions.assertThat(controlBatch);
        batchAssert.isControlBatch(false);
        throwsAssertionErrorContaining(() -> batchAssert.isControlBatch(true), "[controlBatch]");
        controlBatchAssert.isControlBatch(true);
        throwsAssertionErrorContaining(() -> controlBatchAssert.isControlBatch(false), "[controlBatch]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.isControlBatch(false));
    }

    @Test
    void testRecordBatchIsTransactional() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatch transactionalBatch = RecordTestUtils.abortTransactionControlBatch(1);
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        RecordBatchAssert transactionalBatchAssert = KafkaAssertions.assertThat(transactionalBatch);
        batchAssert.isTransactional(false);
        throwsAssertionErrorContaining(() -> batchAssert.isTransactional(true), "[transactional]");
        transactionalBatchAssert.isTransactional(true);
        throwsAssertionErrorContaining(() -> transactionalBatchAssert.isTransactional(false), "[transactional]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.isTransactional(false));
    }

    @Test
    void testRecordBatchHasPartitionLeaderEpoch() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasPartitionLeaderEpoch(0);
        throwsAssertionErrorContaining(() -> batchAssert.hasPartitionLeaderEpoch(1), "[partitionLeaderEpoch]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasPartitionLeaderEpoch(1));
    }

    @Test
    void testRecordBatchHasDeleteHorizonMs() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasDeleteHorizonMs(OptionalLong.empty());
        throwsAssertionErrorContaining(() -> batchAssert.hasDeleteHorizonMs(OptionalLong.of(1L)), "[deleteHorizonMs]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasDeleteHorizonMs(OptionalLong.of(1L)));
    }

    @Test
    void testRecordBatchHasLastOffset() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasLastOffset(0L);
        throwsAssertionErrorContaining(() -> batchAssert.hasLastOffset(1L), "[lastOffset]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasLastOffset(1L));
    }

    @Test
    void testRecordBatchHasLastSequence() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasLastSequence(0);
        throwsAssertionErrorContaining(() -> batchAssert.hasLastSequence(1), "[lastSequence]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasLastSequence(1));
    }

    @Test
    void testRecordBatchHasProducerEpoch() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasProducerEpoch((short) 0);
        throwsAssertionErrorContaining(() -> batchAssert.hasProducerEpoch((short) 1), "[producerEpoch]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasProducerEpoch((short) 1));
    }

    @Test
    void testRecordBatchHasProducerId() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasProducerId(0L);
        throwsAssertionErrorContaining(() -> batchAssert.hasProducerId(1L), "[producerId]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasProducerId(1L));
    }

    @Test
    void testRecordBatchHasMaxTimestamp() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasMaxTimestamp(0L);
        throwsAssertionErrorContaining(() -> batchAssert.hasMaxTimestamp(1L), "[maxTimestamp]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasMaxTimestamp(1L));
    }

    @Test
    void testRecordBatchHasTimestampType() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasTimestampType(TimestampType.CREATE_TIME);
        throwsAssertionErrorContaining(() -> batchAssert.hasTimestampType(TimestampType.LOG_APPEND_TIME), "[timestampType]");
        throwsAssertionErrorContaining(() -> batchAssert.hasTimestampType(null), "[timestampType]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasTimestampType(TimestampType.LOG_APPEND_TIME));
    }

    @Test
    void testRecordBatchHasMetadataMatching() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatch batchSameMetadata = RecordTestUtils.singleElementRecordBatch(
                "KEY",
                "VALUE",
                new RecordHeader("HEADER", "HEADER_VALUE".getBytes(StandardCharsets.UTF_8))
        );
        RecordBatch batchDifferentMetadata = singleElementRecordBatch(
                RecordBatch.CURRENT_MAGIC_VALUE,
                1L,
                Compression.gzip().build(),
                TimestampType.CREATE_TIME,
                1L,
                1L,
                (short) 1,
                1,
                false,
                false,
                1,
                "KEY".getBytes(
                        StandardCharsets.UTF_8
                ),
                "VALUE".getBytes(StandardCharsets.UTF_8),
                new RecordHeader[]{}
        );
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasMetadataMatching(batch);
        batchAssert.hasMetadataMatching(batchSameMetadata);
        throwsAssertionErrorContaining(() -> batchAssert.hasMetadataMatching(batchDifferentMetadata), "[baseOffset]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasMetadataMatching(batch));
    }

    @Test
    void testRecordBatchHasNumRecords() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);
        batchAssert.hasNumRecords(1);
        throwsAssertionErrorContaining(() -> batchAssert.hasNumRecords(2), "[records]");
        assertThrowsIfRecordBatchNull(nullAssert -> nullAssert.hasNumRecords(1));
    }

    @Test
    void testRecordBatchFirstRecord() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatch emptyByCompaction = RecordTestUtils.recordBatchWithAllRecordsRemoved(1L);
        RecordBatch multipleRecordsBatch = RecordTestUtils.memoryRecords(List.of(RecordTestUtils.record(0L, "KEY", "a"), RecordTestUtils.record(1L, "KEY2", "b")))
                                                          .firstBatch();
        KafkaAssertions.assertThat(batch).firstRecord().hasKeyEqualTo("KEY");
        KafkaAssertions.assertThat(multipleRecordsBatch).firstRecord().hasKeyEqualTo("KEY");
        throwsAssertionErrorContaining(() -> KafkaAssertions.assertThat(emptyByCompaction).firstRecord(), "[record batch]");
        assertThrowsIfRecordBatchNull(RecordBatchAssert::firstRecord);
    }

    @Test
    void testRecordBatchLastRecord() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatch emptyByCompaction = RecordTestUtils.recordBatchWithAllRecordsRemoved(1L);
        RecordBatch multipleRecordsBatch = RecordTestUtils.memoryRecords(List.of(RecordTestUtils.record(0L, "KEY", "a"), RecordTestUtils.record(1L, "KEY2", "b")))
                                                          .firstBatch();
        KafkaAssertions.assertThat(batch).lastRecord().hasKeyEqualTo("KEY");
        KafkaAssertions.assertThat(multipleRecordsBatch).lastRecord().hasKeyEqualTo("KEY2");
        throwsAssertionErrorContaining(() -> KafkaAssertions.assertThat(emptyByCompaction).lastRecord(), "[record batch]");
        assertThrowsIfRecordBatchNull(RecordBatchAssert::lastRecord);
    }

    @Test
    void testRecords() {
        RecordBatch batch = RecordTestUtils.singleElementRecordBatch("KEY", "VALUE");
        RecordBatch emptyByCompaction = RecordTestUtils.recordBatchWithAllRecordsRemoved(1L);
        RecordBatch multipleRecordsBatch = RecordTestUtils.memoryRecords(List.of(RecordTestUtils.record(0L, "KEY", "a"), RecordTestUtils.record(1L, "KEY", "b")))
                                                          .firstBatch();
        for (RecordAssert record : KafkaAssertions.assertThat(batch).records()) {
            record.hasKeyEqualTo("KEY");
        }
        for (RecordAssert record : KafkaAssertions.assertThat(multipleRecordsBatch).records()) {
            record.hasKeyEqualTo("KEY");
        }
        for (RecordAssert record : KafkaAssertions.assertThat(emptyByCompaction).records()) {
            record.hasKeyEqualTo("KEY");
        }
        assertThrowsIfRecordBatchNull(RecordBatchAssert::records);
    }

    void assertThrowsIfRecordBatchNull(ThrowingConsumer<RecordBatchAssert> action) {
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat((RecordBatch) null);
        throwsAssertionErrorContaining(() -> action.accept(batchAssert), "[null record batch]");
    }
}
