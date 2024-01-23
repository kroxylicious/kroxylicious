/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;

import io.kroxylicious.test.record.RecordTestUtils;

import static io.kroxylicious.test.assertj.Assertions.throwsAssertionErrorContaining;

class MemoryRecordsAssertTest {

    @Test
    void testHasNumBatches() {
        MemoryRecords records = RecordTestUtils.singleElementMemoryRecords("KEY", "VALUE", new RecordHeader("HEADER", "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        MemoryRecordsAssert recordsAssert = KafkaAssertions.assertThat(records);
        recordsAssert.hasNumBatches(1);
        throwsAssertionErrorContaining(() -> recordsAssert.hasNumBatches(2), "[number of batches]");
        assertThrowsIfMemoryRecordsNull(nullAssert -> nullAssert.hasNumBatches(1));
    }

    @Test
    void testHasBatchSizes() {
        MemoryRecords records = RecordTestUtils.singleElementMemoryRecords("KEY", "VALUE", new RecordHeader("HEADER", "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        MemoryRecordsAssert recordsAssert = KafkaAssertions.assertThat(records);
        recordsAssert.hasBatchSizes(1);
        throwsAssertionErrorContaining(() -> recordsAssert.hasBatchSizes(2), "[batch sizes]");
        assertThrowsIfMemoryRecordsNull(nullAssert -> nullAssert.hasBatchSizes(1));
    }

    @Test
    void testFirstBatch() {
        MemoryRecords records = RecordTestUtils.singleElementMemoryRecords("KEY", "VALUE", new RecordHeader("HEADER", "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        MutableRecordBatch batch1 = RecordTestUtils.singleElementRecordBatch("FIRST", "value");
        MutableRecordBatch batch2 = RecordTestUtils.singleElementRecordBatch("LAST", "value");
        MemoryRecords multiBatch = RecordTestUtils.memoryRecords(batch1, batch2);
        MemoryRecords empty = MemoryRecords.EMPTY;
        KafkaAssertions.assertThat(records).firstBatch().firstRecord().hasKeyEqualTo("KEY");
        KafkaAssertions.assertThat(multiBatch).firstBatch().firstRecord().hasKeyEqualTo("FIRST");
        throwsAssertionErrorContaining(() -> KafkaAssertions.assertThat(empty).firstBatch(), "number of batches");
        assertThrowsIfMemoryRecordsNull(MemoryRecordsAssert::firstBatch);
    }

    @Test
    void testLastBatch() {
        MemoryRecords records = RecordTestUtils.singleElementMemoryRecords("KEY", "VALUE", new RecordHeader("HEADER", "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        MutableRecordBatch batch1 = RecordTestUtils.singleElementRecordBatch("FIRST", "value");
        MutableRecordBatch batch2 = RecordTestUtils.singleElementRecordBatch("LAST", "value");
        MemoryRecords multiBatch = RecordTestUtils.memoryRecords(batch1, batch2);
        MemoryRecords empty = MemoryRecords.EMPTY;
        KafkaAssertions.assertThat(records).lastBatch().firstRecord().hasKeyEqualTo("KEY");
        KafkaAssertions.assertThat(multiBatch).lastBatch().firstRecord().hasKeyEqualTo("LAST");
        throwsAssertionErrorContaining(() -> KafkaAssertions.assertThat(empty).lastBatch(), "number of batches");
        assertThrowsIfMemoryRecordsNull(MemoryRecordsAssert::lastBatch);
    }

    @Test
    void testBatches() {
        MemoryRecords records = RecordTestUtils.singleElementMemoryRecords("KEY", "VALUE", new RecordHeader("HEADER", "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        MutableRecordBatch batch1 = RecordTestUtils.singleElementRecordBatch("KEY", "value");
        MutableRecordBatch batch2 = RecordTestUtils.singleElementRecordBatch("KEY", "value");
        MemoryRecords multiBatch = RecordTestUtils.memoryRecords(batch1, batch2);
        MemoryRecords empty = MemoryRecords.EMPTY;
        for (RecordBatchAssert batch : KafkaAssertions.assertThat(records).batches()) {
            batch.firstRecord().hasKeyEqualTo("KEY");
        }
        for (RecordBatchAssert batch : KafkaAssertions.assertThat(multiBatch).batches()) {
            batch.firstRecord().hasKeyEqualTo("KEY");
        }
        for (RecordBatchAssert batch : KafkaAssertions.assertThat(empty).batches()) {
            batch.firstRecord().hasKeyEqualTo("KEY");
        }
        assertThrowsIfMemoryRecordsNull(MemoryRecordsAssert::batches);
    }

    @Test
    void testHasSizeInBytes() {
        MemoryRecords records = RecordTestUtils.singleElementMemoryRecords("KEY", "VALUE", new RecordHeader("HEADER", "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        MemoryRecordsAssert recordsAssert = KafkaAssertions.assertThat(records);
        recordsAssert.hasSizeInBytes(96);
        throwsAssertionErrorContaining(() -> recordsAssert.hasSizeInBytes(1), "[sizeInBytes]");
        assertThrowsIfMemoryRecordsNull(nullAssert -> nullAssert.hasSizeInBytes(1));
    }

    void assertThrowsIfMemoryRecordsNull(ThrowingConsumer<MemoryRecordsAssert> action) {
        MemoryRecordsAssert headerAssert = KafkaAssertions.assertThat((MemoryRecords) null);
        throwsAssertionErrorContaining(() -> action.accept(headerAssert), "[null memory records]");
    }

}
