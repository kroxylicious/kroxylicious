/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;

public class ConsumerRecordAssert extends AbstractAssert<ConsumerRecordAssert, ConsumerRecord<?, ?>> {
    protected ConsumerRecordAssert(ConsumerRecord consumerRecord) {
        super(consumerRecord, ConsumerRecordAssert.class);
    }

    public static ConsumerRecordAssert assertThat(ConsumerRecord<?, ?> actual) {
        return new ConsumerRecordAssert(actual);
    }

    public HeadersAssert headers() {
        isNotNull();
        return extracting(ConsumerRecord::headers,
                new InstanceOfAssertFactory<>(Headers.class, HeadersAssert::assertThat));
    }
}
