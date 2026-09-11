/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.kroxylicious.kafka.common.header.Header;
import io.kroxylicious.kafka.common.header.Headers;
import io.kroxylicious.kafka.common.record.internal.MemoryRecords;
import io.kroxylicious.kafka.common.record.internal.Record;
import io.kroxylicious.kafka.common.record.internal.RecordBatch;

/**
 * Entry point for AssertJ assertions on Kafka record types.
 */
public class KafkaAssertions {
    private KafkaAssertions() {
    }

    // Assertions

    /**
     * Creates an assertion for the given MemoryRecords.
     *
     * @param actual the actual MemoryRecords
     * @return the assertion
     */
    public static MemoryRecordsAssert assertThat(MemoryRecords actual) {
        return MemoryRecordsAssert.assertThat(actual);
    }

    /**
     * Creates an assertion for the given RecordBatch.
     *
     * @param actual the actual RecordBatch
     * @return the assertion
     */
    public static RecordBatchAssert assertThat(RecordBatch actual) {
        return RecordBatchAssert.assertThat(actual);
    }

    /**
     * Creates an assertion for the given Record.
     *
     * @param actual the actual Record
     * @return the assertion
     */
    public static RecordAssert assertThat(Record actual) {
        return RecordAssert.assertThat(actual);
    }

    /**
     * Creates an assertion for the given ConsumerRecord.
     *
     * @param actual the actual ConsumerRecord
     * @return the assertion
     */
    public static ConsumerRecordAssert assertThat(ConsumerRecord<?, ?> actual) {
        return ConsumerRecordAssert.assertThat(actual);
    }

    /**
     * Creates an assertion for the given Headers.
     *
     * @param actual the actual Headers
     * @return the assertion
     */
    public static HeadersAssert assertThat(Headers actual) {
        return HeadersAssert.assertThat(actual);
    }

    /**
     * Creates an assertion for the given Header.
     *
     * @param actual the actual Header
     * @return the assertion
     */
    public static HeaderAssert assertThat(Header actual) {
        return HeaderAssert.assertThat(actual);
    }

}
