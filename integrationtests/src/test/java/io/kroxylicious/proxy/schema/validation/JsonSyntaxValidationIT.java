/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.schema.validation;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.BaseIT;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.proxy.filter.schema.ProduceValidationFilterFactory;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(KafkaClusterExtension.class)
class JsonSyntaxValidationIT extends BaseIT {

    public static final String SYNTACTICALLY_CORRECT_JSON = "{\"value\":\"json\"}";
    public static final String SYNTACTICALLY_INCORRECT_JSON = "Not Json";
    private static final String TOPIC_1 = "my-test-topic";
    private static final String TOPIC_2 = "my-test-topic-2";

    @Test
    void testInvalidJsonProduceRejected(KafkaCluster cluster, Admin admin) throws Exception {
        assertThat(cluster.getNumOfBrokers()).isOne();
        createTopic(admin, TOPIC_1, 1);

        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName()).withConfig("rules",
                        List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                                Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true)))))
                        .build());
        try (var tester = kroxyliciousTester(config);
                var producer = getProducer(tester, 0, 16384)) {
            Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
            assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
        }
    }

    @Test
    void testInvalidJsonProduceRejectedUsingTopicNames(KafkaCluster cluster, Admin admin) throws Exception {
        assertThat(cluster.getNumOfBrokers()).isOne();
        createTopics(admin, new NewTopic(TOPIC_1, 1, (short) 1), new NewTopic(TOPIC_2, 1, (short) 1));

        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName()).withConfig("rules",
                        List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                                Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true)))))
                        .build());
        try (var tester = kroxyliciousTester(config);
                var producer = getProducer(tester, 0, 16384);
                var consumer = getConsumer(tester)) {
            Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
            assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
            producer.send(new ProducerRecord<>(TOPIC_2, "my-key", SYNTACTICALLY_INCORRECT_JSON)).get();
            consumer.subscribe(Set.of(TOPIC_2));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isOne();
            assertThat(records.iterator().next().value()).isEqualTo(SYNTACTICALLY_INCORRECT_JSON);
        }
    }

    @Test
    void testPartiallyInvalidJsonTransactionalAllRejected(KafkaCluster cluster, Admin admin) throws Exception {
        assertThat(cluster.getNumOfBrokers()).isOne();
        createTopics(admin, new NewTopic(TOPIC_1, 1, (short) 1), new NewTopic(TOPIC_2, 1, (short) 1));

        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName()).withConfig("forwardPartialRequests", true, "rules",
                        List.of(Map.of("topicNames", List.of(TOPIC_1, TOPIC_2), "valueRule",
                                Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true)))))
                        .build());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(LINGER_MS_CONFIG, 5000, TRANSACTIONAL_ID_CONFIG, randomUUID().toString()))) {
            producer.initTransactions();
            producer.beginTransaction();
            Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
            Future<RecordMetadata> valid = producer.send(new ProducerRecord<>(TOPIC_2, "my-key", SYNTACTICALLY_CORRECT_JSON));
            producer.flush();
            assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
            assertInvalidRecordExceptionThrown(valid, "Invalid record in another topic-partition caused whole ProduceRequest to be invalidated");
            producer.abortTransaction();
        }
    }

    @Test
    void testPartiallyInvalidJsonNotConfiguredToForwardAllRejected(KafkaCluster cluster, Admin admin) throws Exception {
        assertThat(cluster.getNumOfBrokers()).isOne();
        createTopics(admin, new NewTopic(TOPIC_1, 1, (short) 1), new NewTopic(TOPIC_2, 1, (short) 1));

        boolean forwardPartialRequests = false;
        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName())
                        .withConfig("forwardPartialRequests", forwardPartialRequests, "rules",
                                List.of(Map.of("topicNames", List.of(TOPIC_1, TOPIC_2), "valueRule",
                                        Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true)))))
                        .build());

        try (var tester = kroxyliciousTester(config);
                var producer = getProducer(tester, 5000, 16384)) {
            Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
            Future<RecordMetadata> valid = producer.send(new ProducerRecord<>(TOPIC_2, "my-key", SYNTACTICALLY_CORRECT_JSON));
            producer.flush();
            assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
            assertInvalidRecordExceptionThrown(valid, "Invalid record in another topic-partition caused whole ProduceRequest to be invalidated");
        }
    }

    @Test
    void testPartiallyInvalidJsonProduceRejected(KafkaCluster cluster, Admin admin) throws Exception {
        assertThat(cluster.getNumOfBrokers()).isOne();
        createTopics(admin, new NewTopic(TOPIC_1, 1, (short) 1), new NewTopic(TOPIC_2, 1, (short) 1));

        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName())
                        .withConfig("forwardPartialRequests", true,
                                "rules", List.of(Map.of("topicNames", List.of(TOPIC_1, TOPIC_2), "valueRule",
                                        Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true)))))
                        .build());

        try (var tester = kroxyliciousTester(config);
                var producer = getProducer(tester, 5000, 16384);
                var consumer = getConsumer(tester)) {
            Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
            Future<RecordMetadata> valid = producer.send(new ProducerRecord<>(TOPIC_2, "my-key", SYNTACTICALLY_CORRECT_JSON));
            producer.flush();
            assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
            RecordMetadata metadata = valid.get(10, TimeUnit.SECONDS);
            assertThat(metadata.hasOffset()).isTrue();

            consumer.subscribe(Set.of(TOPIC_2));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isOne();
            assertThat(records.iterator().next().value()).isEqualTo(SYNTACTICALLY_CORRECT_JSON);
        }
    }

    @Test
    void testPartiallyInvalidAcrossPartitionsOfSameTopic(KafkaCluster cluster, Admin admin) throws Exception {
        assertThat(cluster.getNumOfBrokers()).isOne();
        createTopic(admin, TOPIC_1, 2);

        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName())
                        .withConfig("forwardPartialRequests", true,
                                "rules", List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                                        Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true)))))
                        .build());

        try (var tester = kroxyliciousTester(config);
                var producer = getProducer(tester, 5000, 16384);
                var consumer = getConsumer(tester)) {
            Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, 0, "my-key", SYNTACTICALLY_INCORRECT_JSON));
            Future<RecordMetadata> valid = producer.send(new ProducerRecord<>(TOPIC_1, 1, "my-key", SYNTACTICALLY_CORRECT_JSON));
            producer.flush();
            assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
            RecordMetadata metadata = valid.get(10, TimeUnit.SECONDS);
            assertThat(metadata.hasOffset()).isTrue();

            consumer.subscribe(Set.of(TOPIC_1));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isOne();
            assertThat(records.iterator().next().value()).isEqualTo(SYNTACTICALLY_CORRECT_JSON);
        }
    }

    @Test
    void testPartiallyInvalidWithinOnePartitionOfTopic(KafkaCluster cluster, Admin admin) throws Exception {
        assertThat(cluster.getNumOfBrokers()).isOne();
        createTopic(admin, TOPIC_1, 1);

        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName()).withConfig("forwardPartialRequests", true, "rules",
                        List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                                Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true)))))
                        .build());

        try (var tester = kroxyliciousTester(config);
                var producer = getProducer(tester, 5000, 16384)) {
            Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
            Future<RecordMetadata> valid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_CORRECT_JSON));
            producer.flush();
            assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
            assertThatThrownBy(() -> {
                valid.get(10, TimeUnit.SECONDS);
            }).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(KafkaException.class).cause()
                    .hasMessageContaining("Failed to append record because it was part of a batch which had one more more invalid records");
        }
    }

    @Test
    void testValidJsonProduceAccepted(KafkaCluster cluster, Admin admin) throws Exception {
        assertThat(cluster.getNumOfBrokers()).isOne();
        createTopic(admin, TOPIC_1, 1);

        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName()).withConfig("rules",
                        List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                                Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true)))))
                        .build());

        try (var tester = kroxyliciousTester(config);
                var producer = getProducer(tester, 0, 16384)) {
            producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_CORRECT_JSON)).get();
        }
    }

    private Producer<String, String> getProducer(KroxyliciousTester tester, int linger, int batchSize) {
        return getProducerWithConfig(tester, Optional.empty(), Map.of(LINGER_MS_CONFIG, linger, ProducerConfig.BATCH_SIZE_CONFIG, batchSize));
    }

    private Consumer<String, String> getConsumer(KroxyliciousTester tester) {
        return getConsumerWithConfig(tester, Optional.empty(), Map.of(GROUP_ID_CONFIG, "my-group-id", AUTO_OFFSET_RESET_CONFIG, "earliest"));
    }

    private static void assertInvalidRecordExceptionThrown(Future<RecordMetadata> invalid, String message) {
        assertThatThrownBy(() -> {
            invalid.get(10, TimeUnit.SECONDS);
        }).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(InvalidRecordException.class).cause()
                .hasMessageContaining(message);
    }

}
