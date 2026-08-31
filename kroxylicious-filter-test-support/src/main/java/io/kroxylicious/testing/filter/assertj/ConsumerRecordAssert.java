/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;

import io.kroxylicious.kafka.common.header.Headers;

/**
 * AssertJ assertions for {@link ConsumerRecord}.
 */
public class ConsumerRecordAssert extends AbstractAssert<ConsumerRecordAssert, ConsumerRecord<?, ?>> {
    /**
     * Constructs an assertion for the given ConsumerRecord.
     *
     * @param consumerRecord the actual ConsumerRecord
     */
    protected ConsumerRecordAssert(ConsumerRecord<?, ?> consumerRecord) {
        super(consumerRecord, ConsumerRecordAssert.class);
    }

    /**
     * Creates an assertion for the given ConsumerRecord.
     *
     * @param actual the actual ConsumerRecord
     * @return the assertion
     */
    public static ConsumerRecordAssert assertThat(ConsumerRecord<?, ?> actual) {
        return new ConsumerRecordAssert(actual);
    }

    /**
     * Creates an assertion for the record's headers.
     *
     * @return the headers assertion
     */
    public HeadersAssert headers() {
        isNotNull();
        return extracting(ConsumerRecord::headers,
                new InstanceOfAssertFactory<>(Headers.class, HeadersAssert::assertThat));
    }
}
