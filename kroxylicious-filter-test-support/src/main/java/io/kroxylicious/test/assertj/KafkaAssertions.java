/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

public class KafkaAssertions {
    private KafkaAssertions() {
    }

    // Assertions

    public static MemoryRecordsAssert assertThat(MemoryRecords actual) {
        return MemoryRecordsAssert.assertThat(actual);
    }

    public static RecordBatchAssert assertThat(RecordBatch actual) {
        return RecordBatchAssert.assertThat(actual);
    }

    public static RecordAssert assertThat(Record actual) {
        return RecordAssert.assertThat(actual);
    }

    public static ConsumerRecordAssert assertThat(ConsumerRecord actual) {
        return ConsumerRecordAssert.assertThat(actual);
    }

    public static HeadersAssert assertThat(Headers actual) {
        return HeadersAssert.assertThat(actual);
    }

    public static HeaderAssert assertThat(Header actual) {
        return HeaderAssert.assertThat(actual);
    }

}
