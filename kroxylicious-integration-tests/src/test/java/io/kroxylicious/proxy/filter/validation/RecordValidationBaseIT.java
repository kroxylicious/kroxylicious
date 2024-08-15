/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.InvalidRecordException;

import io.kroxylicious.proxy.BaseIT;
import io.kroxylicious.test.tester.KroxyliciousTester;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class RecordValidationBaseIT extends BaseIT {

    protected Producer<String, String> getProducer(KroxyliciousTester tester, int linger, int batchSize) {
        return getProducerWithConfig(tester, Optional.empty(), Map.of(LINGER_MS_CONFIG, linger, ProducerConfig.BATCH_SIZE_CONFIG, batchSize));
    }

    protected org.apache.kafka.clients.consumer.Consumer<String, String> getConsumer(KroxyliciousTester tester) {
        return getConsumerWithConfig(tester, Optional.empty(), Map.of(GROUP_ID_CONFIG, "my-group-id", AUTO_OFFSET_RESET_CONFIG, "earliest"));
    }

    protected static void assertInvalidRecordExceptionThrown(Future<RecordMetadata> invalid, String message) {
        assertThatThrownBy(() -> {
            invalid.get(10, TimeUnit.SECONDS);
        }).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(InvalidRecordException.class).cause()
                .hasMessageContaining(message);
    }

}
