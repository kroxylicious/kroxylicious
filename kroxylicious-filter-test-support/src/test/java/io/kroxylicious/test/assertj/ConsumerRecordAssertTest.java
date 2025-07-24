/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

public class ConsumerRecordAssertTest {

    long timestamp = System.currentTimeMillis();

    ConsumerRecord<String, String> record = new ConsumerRecord<>("topic",
            3,
            42L,
            timestamp,
            TimestampType.LOG_APPEND_TIME,
            ConsumerRecord.NULL_SIZE,
            ConsumerRecord.NULL_SIZE,
            "my key",
            "my value",
            new RecordHeaders().add("my header", "my header value".getBytes(StandardCharsets.UTF_8)),
            Optional.of(128));

    @Test
    void headers() {
        KafkaAssertions.assertThat(record).headers().hasSize(1);
    }
}
