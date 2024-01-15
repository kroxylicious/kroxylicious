/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import io.kroxylicious.test.record.RecordTestUtils;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.Test;

import static io.kroxylicious.test.record.RecordTestUtils.memoryRecords;
import static io.kroxylicious.test.record.RecordTestUtils.record;
import static io.kroxylicious.test.record.RecordTestUtils.recordBatch;
import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.Assertions;

import java.nio.charset.StandardCharsets;

class KafkaAssertionsTest {

    @Test
    void testHeader() {
        RecordHeader header = new RecordHeader("foo", null);
        HeaderAssert headerAssert = KafkaAssertions.assertThat(header);

        // key
        headerAssert.hasKeyEqualTo("foo");
        Assertions.assertThatThrownBy(() -> headerAssert.hasKeyEqualTo("bar")).hasMessageContaining("[header key]");

        // value
        headerAssert.hasNullValue();
        HeaderAssert headerAssert1 = KafkaAssertions.assertThat(new RecordHeader("foo", new byte[0]));
        Assertions.assertThatThrownBy(() -> headerAssert1.hasNullValue()).hasMessageContaining("[header value]");
        HeaderAssert headerAssert2 = KafkaAssertions.assertThat(new RecordHeader("foo", "abc".getBytes(StandardCharsets.UTF_8)));
        headerAssert2.hasValueEqualTo("abc");
        Assertions.assertThatThrownBy(() -> headerAssert2.hasValueEqualTo("xyz")).hasMessageContaining("[header value]");

    }

    @Test
    void testRecord() {
        Record record = record("KEY", "VALUE", new RecordHeader("HEADER", "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        RecordAssert recordAssert = KafkaAssertions.assertThat(record);

        // offset
        recordAssert.hasOffsetEqualTo(0);
        Assertions.assertThatThrownBy(() -> recordAssert.hasOffsetEqualTo(1)).hasMessageContaining("[record offset]");

        // timestamp
        recordAssert.hasTimestampEqualTo(0);
        Assertions.assertThatThrownBy(() -> recordAssert.hasTimestampEqualTo(1)).hasMessageContaining("[record timestamp]");

        // key
        recordAssert.hasKeyEqualTo("KEY");
        Assertions.assertThatThrownBy(() -> recordAssert.hasKeyEqualTo("NOT_KEY")).hasMessageContaining("[record key]");
        Assertions.assertThatThrownBy(() -> recordAssert.hasNullKey()).hasMessageContaining("[record key]");

        // value
        recordAssert.hasValueEqualTo("VALUE");
        Assertions.assertThatThrownBy(() -> recordAssert.hasValueEqualTo("NOT_VALUE")).hasMessageContaining("[record value]");
        Assertions.assertThatThrownBy(() -> recordAssert.hasNullValue()).hasMessageContaining("[record value]");

        // headers
        recordAssert.hasHeadersSize(1);
        recordAssert.containsHeaderWithKey("HEADER");
        recordAssert.firstHeader().hasKeyEqualTo("HEADER");
        recordAssert.lastHeader().hasKeyEqualTo("HEADER");
        Assertions.assertThatThrownBy(() -> recordAssert.hasHeadersSize(2)).hasMessageContaining("[record headers]");
        Assertions.assertThatThrownBy(() -> recordAssert.hasEmptyHeaders()).hasMessageContaining("[record headers]");
        Assertions.assertThatThrownBy(() -> recordAssert.containsHeaderWithKey("NOT_HEADER")).hasMessageContaining("[record headers]");
        Assertions.assertThatThrownBy(() -> recordAssert.firstHeader().hasKeyEqualTo("NOT_HEADER")).hasMessageContaining("[header key]");
        Assertions.assertThatThrownBy(() -> recordAssert.lastHeader().hasKeyEqualTo("NOT_HEADER")).hasMessageContaining("[header key]");
    }

    @Test
    void testRecordBatch() {
        RecordBatch batch = recordBatch("KEY", "VALUE", new RecordHeader("HEADER", "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        RecordBatchAssert batchAssert = KafkaAssertions.assertThat(batch);

        // sizeInBytes
        batchAssert.hasSizeInBytes(96);
        Assertions.assertThatThrownBy(() -> batchAssert.hasSizeInBytes(1)).hasMessageContaining("[sizeInBytes]");

        // Base offset
        batchAssert.hasBaseOffset(0);
        Assertions.assertThatThrownBy(() -> batchAssert.hasBaseOffset(1)).hasMessageContaining("[baseOffset]");

        // Base sequence
        batchAssert.hasBaseSequence(0);
        Assertions.assertThatThrownBy(() -> batchAssert.hasBaseSequence(1)).hasMessageContaining("[baseSequence]");

        // compression type
        batchAssert.hasCompressionType(CompressionType.NONE);
        Assertions.assertThatThrownBy(() -> batchAssert.hasCompressionType(CompressionType.GZIP)).hasMessageContaining("[compressionType]");

        // records
        batchAssert.hasNumRecords(1);
        batchAssert.firstRecord().hasKeyEqualTo("KEY");
        batchAssert.lastRecord().hasKeyEqualTo("KEY");
        Assertions.assertThatThrownBy(() -> batchAssert.hasNumRecords(2)).hasMessageContaining("[records]");
    }
    @Test
    void testMemoryRecords() {
        MemoryRecords records = memoryRecords("KEY", "VALUE", new RecordHeader("HEADER", "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        MemoryRecordsAssert recordsAssert = KafkaAssertions.assertThat(records);

        // Num batches
        recordsAssert.hasNumBatches(1);
        Assertions.assertThatThrownBy(() -> recordsAssert.hasNumBatches(2)).hasMessageContaining("[number of batches]");

        // Batch sizes
        recordsAssert.hasBatchSizes(1);
        recordsAssert.firstBatch().firstRecord().hasKeyEqualTo("KEY");
        recordsAssert.lastBatch().firstRecord().hasKeyEqualTo("KEY");
        Assertions.assertThatThrownBy(() -> recordsAssert.hasBatchSizes(2)).hasMessageContaining("[batch sizes]");

        // sizeInByte
        recordsAssert.hasSizeInBytes(96);
        Assertions.assertThatThrownBy(() -> recordsAssert.hasSizeInBytes(1)).hasMessageContaining("[sizeInBytes]");
    }
}