/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests with the aim of demonstrating that system survives a Kroxylicious restart.
 */
@ExtendWith(KafkaClusterExtension.class)
class ResilienceIT extends BaseIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResilienceIT.class);

    static @BrokerCluster(numBrokers = 3) KafkaCluster cluster;

    @Test
    void kafkaProducerShouldTolerateKroxyliciousRestarting(Topic randomTopic) throws Exception {
        testProducerCanSurviveARestart(proxy(cluster), randomTopic);
    }

    @Test
    void kafkaConsumerShouldTolerateKroxyliciousRestarting(Topic randomTopic) throws Exception {
        testConsumerCanSurviveKroxyliciousRestart(proxy(cluster), randomTopic);
    }

    private static void testConsumerCanSurviveKroxyliciousRestart(ConfigurationBuilder builder, Topic randomTopic)
                                                                                                                   throws Exception {
        var producerConfig = new HashMap<String, Object>(
                Map.of(
                        CLIENT_ID_CONFIG,
                        "producer",
                        DELIVERY_TIMEOUT_MS_CONFIG,
                        3_600_000
                )
        );
        var consumerConfig = new HashMap<String, Object>(
                Map.of(
                        CLIENT_ID_CONFIG,
                        "consumer",
                        ConsumerConfig.GROUP_ID_CONFIG,
                        "mygroup",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest"
                )
        );

        Consumer<String, String> consumer;
        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer(producerConfig)) {
            consumer = tester.consumer(consumerConfig);
            producer.send(new ProducerRecord<>(randomTopic.name(), "my-key", "Hello, world!")).get(10, TimeUnit.SECONDS);
            consumer.subscribe(Set.of(randomTopic.name()));
            var firstRecords = consumer.poll(Duration.ofSeconds(10));
            assertThat(firstRecords).hasSize(1);
            assertThat(firstRecords.iterator()).toIterable().map(ConsumerRecord::value).containsExactly("Hello, world!");

            producer.send(new ProducerRecord<>(randomTopic.name(), "my-key", "Hello, again!")).get(10, TimeUnit.SECONDS);

            LOGGER.debug("Restarting proxy");
            producer.close();
            tester.restartProxy();

            var secondRecords = consumer.poll(Duration.ofSeconds(10));
            assertThat(secondRecords).hasSize(1);
            assertThat(secondRecords.iterator()).toIterable().map(ConsumerRecord::value).containsExactly("Hello, again!");
            consumer.close();
        }
    }

    private void testProducerCanSurviveARestart(ConfigurationBuilder builder, Topic randomTopic) throws Exception {

        var producerConfig = new HashMap<String, Object>(
                Map.of(
                        CLIENT_ID_CONFIG,
                        "producer",
                        DELIVERY_TIMEOUT_MS_CONFIG,
                        3_600_000,
                        RECONNECT_BACKOFF_MS_CONFIG,
                        5,
                        RECONNECT_BACKOFF_MAX_MS_CONFIG,
                        100,
                        RETRY_BACKOFF_MS_CONFIG,
                        0
                )
        );
        var consumerConfig = new HashMap<String, Object>(
                Map.of(
                        CLIENT_ID_CONFIG,
                        "consumer",
                        ConsumerConfig.GROUP_ID_CONFIG,
                        "mygroup",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest"
                )
        );

        Producer<String, String> producer = null;
        Consumer<String, String> consumer = null;

        try {
            try (var tester = kroxyliciousTester(builder)) {
                producer = tester.producer(producerConfig);
                consumer = tester.consumer(consumerConfig);
                consumer.subscribe(Set.of(randomTopic.name()));
                var response = producer.send(new ProducerRecord<>(randomTopic.name(), "my-key", "Hello, world!")).get(10, TimeUnit.SECONDS);

                LOGGER.debug("Restarting proxy");
                tester.restartProxy();
                // re-use the existing producer and consumer (made through Kroxylicious's first incarnation). This provides us the assurance
                // that they were able to reconnect successfully.
                producer.send(new ProducerRecord<>(randomTopic.name(), "my-key", "Hello, again!")).get(10, TimeUnit.SECONDS);
                producer.close();
                var records = consumer.poll(Duration.ofSeconds(20));
                consumer.close();
                assertThat(records).hasSize(2);
                assertThat(records.iterator()).toIterable().map(ConsumerRecord::value).containsExactly("Hello, world!", "Hello, again!");
            }
        }
        finally {
            try {
                if (producer != null) {
                    producer.close();
                }
            }
            finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
    }
}
