/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.kroxylicious.proxy.BaseIT;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class RecordValidationBaseIT extends BaseIT {

    public void assertThatFutureSucceeds(Future<RecordMetadata> future) {
        assertThat(future)
                          .succeedsWithin(Duration.ofSeconds(5))
                          .isNotNull();
    }

    public void assertThatFutureFails(Future<RecordMetadata> rejected, Class<? extends Throwable> expectedCause, String expectedMessage) {
        assertThat(rejected)
                            .failsWithin(Duration.ofSeconds(5))
                            .withThrowableThat()
                            .withCauseInstanceOf(expectedCause)
                            .withMessageContaining(expectedMessage);
    }

    public ConsumerRecords<String, String> consumeAll(KroxyliciousTester tester, Topic topic) {
        try (var consumer = tester.consumer(Map.of(GROUP_ID_CONFIG, UUID.randomUUID().toString(), AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            consumer.subscribe(Set.of(topic.name()));
            return consumer.poll(Duration.ofSeconds(10));
        }
    }
}
