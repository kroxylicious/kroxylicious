/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.schema.validation;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.KroxyConfig;
import io.kroxylicious.proxy.KroxyConfigBuilder;
import io.kroxylicious.proxy.VirtualClusterBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.proxy.Utils.startProxy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(KafkaClusterExtension.class)
public class JsonSyntaxValidationIT {

    public static final String SYNTACTICALLY_CORRECT_JSON = "{\"value\":\"json\"}";
    public static final String SYNTACTICALLY_INCORRECT_JSON = "Not Json";
    private static final String TOPIC_1 = "my-test-topic";
    private static final String TOPIC_2 = "my-test-topic-2";

    @NotNull
    private static KafkaProducer<String, String> getProducer(String proxyAddress, int linger) {
        return getProducer(proxyAddress, Map.of(ProducerConfig.LINGER_MS_CONFIG, linger));
    }

    private static KafkaProducer<String, String> getProducer(String proxyAddress, Map<String, Object> additionalProps) {
        Map<String, Object> produceProps = new java.util.HashMap<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress, ProducerConfig.CLIENT_ID_CONFIG,
                "shouldModifyProduceMessage",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
        produceProps.putAll(additionalProps);
        return new KafkaProducer<>(produceProps);
    }

    private static KroxyConfigBuilder baseConfigBuilder(String proxyAddress, String bootstrapServers) {
        return KroxyConfig.builder().addToVirtualClusters("demo",
                new VirtualClusterBuilder().withNewTargetCluster().withBootstrapServers(bootstrapServers).endTargetCluster().withNewClusterEndpointConfigProvider()
                        .withType("StaticCluster").withConfig(Map.of("bootstrapAddress", proxyAddress)).endClusterEndpointConfigProvider().build())
                .addNewFilter()
                .withType("ApiVersions").endFilter().addNewFilter().withType("BrokerAddress").endFilter();
    }

    @Test
    public void testInvalidJsonProduceRejected(KafkaCluster cluster, Admin admin) throws Exception {
        String proxyAddress = "localhost:9192";

        admin.createTopics(List.of(new NewTopic(TOPIC_1, 1, (short) 1))).all().get();

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers()).addNewFilter().withType("ProduceValidator").withConfig(Map.of("rules",
                List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                        Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true))))))
                .endFilter()
                .build().toYaml();

        try (var ignored = startProxy(config)) {
            try (var producer = getProducer(proxyAddress, 0)) {
                Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
                assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
            }
        }
    }

    @Test
    public void testInvalidJsonProduceRejectedUsingTopicNames(KafkaCluster cluster, Admin admin) throws Exception {
        String proxyAddress = "localhost:9192";

        admin.createTopics(List.of(new NewTopic(TOPIC_1, 1, (short) 1), new NewTopic(TOPIC_2, 1, (short) 1))).all().get();

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers()).addNewFilter().withType("ProduceValidator").withConfig(Map.of("rules",
                List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                        Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true))))))
                .endFilter()
                .build().toYaml();

        try (var ignored = startProxy(config)) {
            try (var producer = getProducer(proxyAddress, 0)) {
                Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
                assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");

                producer.send(new ProducerRecord<>(TOPIC_2, "my-key", SYNTACTICALLY_INCORRECT_JSON)).get();
                try (var consumer = new KafkaConsumer<String, String>(
                        Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
                    consumer.subscribe(Set.of(TOPIC_2));
                    var records = consumer.poll(Duration.ofSeconds(10));
                    assertEquals(1, records.count());
                    assertEquals(SYNTACTICALLY_INCORRECT_JSON, records.iterator().next().value());
                }
            }
        }
    }

    @Test
    public void testPartiallyInvalidJsonTransactionalAllRejected(KafkaCluster cluster, Admin admin) throws Exception {
        String proxyAddress = "localhost:9192";

        admin.createTopics(List.of(new NewTopic(TOPIC_1, 1, (short) 1), new NewTopic(TOPIC_2, 1, (short) 1))).all().get();

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers()).addNewFilter().withType("ProduceValidator").withConfig(
                Map.of("forwardPartialRequests", true, "rules", List.of(Map.of("topicNames", List.of(TOPIC_1, TOPIC_2), "valueRule",
                        Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true))))))
                .endFilter()
                .build().toYaml();

        try (var ignored = startProxy(config)) {
            try (var producer = getProducer(proxyAddress,
                    Map.of(ProducerConfig.LINGER_MS_CONFIG, 100, ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString()))) {
                producer.initTransactions();
                producer.beginTransaction();
                // leans on linger.ms to try and get two topic-partition batches in the same ProduceRequest, but it's not determistic
                // could be tested deterministically with the MockServer
                Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
                Future<RecordMetadata> valid = producer.send(new ProducerRecord<>(TOPIC_2, "my-key", SYNTACTICALLY_CORRECT_JSON));
                assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
                assertInvalidRecordExceptionThrown(valid, "Invalid record in another topic-partition caused whole ProduceRequest to be invalidated");
                producer.abortTransaction();
            }
        }
    }

    private static void assertInvalidRecordExceptionThrown(Future<RecordMetadata> invalid, String message) {
        Assertions.assertThatThrownBy(() -> {
            invalid.get(10, TimeUnit.SECONDS);
        }).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(InvalidRecordException.class).cause()
                .hasMessageContaining(message);
    }

    @Test
    public void testPartiallyInvalidJsonNotConfiguredToForwardAllRejected(KafkaCluster cluster, Admin admin) throws Exception {
        String proxyAddress = "localhost:9192";

        admin.createTopics(List.of(new NewTopic(TOPIC_1, 1, (short) 1), new NewTopic(TOPIC_2, 1, (short) 1))).all().get();

        boolean forwardPartialRequests = false;
        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers()).addNewFilter().withType("ProduceValidator").withConfig(
                Map.of("forwardPartialRequests", forwardPartialRequests, "rules", List.of(Map.of("topicNames", List.of(TOPIC_1, TOPIC_2), "valueRule",
                        Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true))))))
                .endFilter()
                .build().toYaml();

        try (var ignored = startProxy(config)) {
            try (var producer = getProducer(proxyAddress, 100)) {
                // leans on linger.ms to try and get two topic-partition batches in the same ProduceRequest, but it's not determistic
                // could be tested deterministically with the MockServer
                Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
                Future<RecordMetadata> valid = producer.send(new ProducerRecord<>(TOPIC_2, "my-key", SYNTACTICALLY_CORRECT_JSON));
                assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
                assertInvalidRecordExceptionThrown(valid, "Invalid record in another topic-partition caused whole ProduceRequest to be invalidated");
            }
        }
    }

    @Test
    public void testPartiallyInvalidJsonProduceRejected(KafkaCluster cluster, Admin admin) throws Exception {
        String proxyAddress = "localhost:9192";

        admin.createTopics(List.of(new NewTopic(TOPIC_1, 1, (short) 1), new NewTopic(TOPIC_2, 1, (short) 1))).all().get();

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers()).addNewFilter().withType("ProduceValidator").withConfig(
                Map.of("forwardPartialRequests", true, "rules", List.of(Map.of("topicNames", List.of(TOPIC_1, TOPIC_2), "valueRule",
                        Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true))))))
                .endFilter()
                .build().toYaml();

        try (var ignored = startProxy(config)) {
            try (var producer = getProducer(proxyAddress, 100)) {
                // leans on linger.ms to try and get two topic-partition batches in the same ProduceRequest, but it's not determistic
                // could be tested deterministically with the MockServer
                Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
                Future<RecordMetadata> valid = producer.send(new ProducerRecord<>(TOPIC_2, "my-key", SYNTACTICALLY_CORRECT_JSON));
                assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
                RecordMetadata metadata = valid.get(10, TimeUnit.SECONDS);
                assertTrue(metadata.hasOffset());
            }

            try (var consumer = new KafkaConsumer<String, String>(
                    Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
                consumer.subscribe(Set.of(TOPIC_2));
                var records = consumer.poll(Duration.ofSeconds(10));
                assertEquals(1, records.count());
                assertEquals(SYNTACTICALLY_CORRECT_JSON, records.iterator().next().value());
            }
        }
    }

    @Test
    public void testPartiallyInvalidAcrossPartitionsOfSameTopic(KafkaCluster cluster, Admin admin) throws Exception {
        String proxyAddress = "localhost:9192";

        admin.createTopics(List.of(new NewTopic(TOPIC_1, 2, (short) 1))).all().get();

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers()).addNewFilter().withType("ProduceValidator").withConfig(
                Map.of("forwardPartialRequests", true, "rules", List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                        Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true))))))
                .endFilter()
                .build().toYaml();

        try (var ignored = startProxy(config)) {
            try (var producer = getProducer(proxyAddress, 100)) {
                // leans on linger.ms to try and get two topic-partition batches in the same ProduceRequest, but it's not determistic
                // could be tested deterministically with the MockServer
                Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, 0, "my-key", SYNTACTICALLY_INCORRECT_JSON));
                Future<RecordMetadata> valid = producer.send(new ProducerRecord<>(TOPIC_1, 1, "my-key", SYNTACTICALLY_CORRECT_JSON));
                assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
                RecordMetadata metadata = valid.get(10, TimeUnit.SECONDS);
                assertTrue(metadata.hasOffset());
            }

            try (var consumer = new KafkaConsumer<String, String>(
                    Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
                consumer.subscribe(Set.of(TOPIC_1));
                var records = consumer.poll(Duration.ofSeconds(10));
                assertEquals(1, records.count());
                assertEquals(SYNTACTICALLY_CORRECT_JSON, records.iterator().next().value());
            }
        }
    }

    @Test
    public void testPartiallyInvalidWithinOnePartitionOfTopic(KafkaCluster cluster, Admin admin) throws Exception {
        String proxyAddress = "localhost:9192";

        admin.createTopics(List.of(new NewTopic(TOPIC_1, 1, (short) 1))).all().get();

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers()).addNewFilter().withType("ProduceValidator").withConfig(
                Map.of("forwardPartialRequests", true, "rules", List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                        Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true))))))
                .endFilter()
                .build().toYaml();

        try (var ignored = startProxy(config)) {
            try (var producer = getProducer(proxyAddress, 100)) {
                // leans on linger.ms to try and get two topic-partition batches in the same ProduceRequest, but it's not determistic
                // could be tested deterministically with the MockServer
                Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_INCORRECT_JSON));
                Future<RecordMetadata> valid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_CORRECT_JSON));
                assertInvalidRecordExceptionThrown(invalid, "value was not syntactically correct JSON");
                Assertions.assertThatThrownBy(() -> {
                    valid.get(10, TimeUnit.SECONDS);
                }).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(KafkaException.class).cause()
                        .hasMessageContaining("Failed to append record because it was part of a batch which had one more more invalid records");
            }
        }
    }

    @Test
    public void testValidJsonProduceAccepted(KafkaCluster cluster, Admin admin) throws Exception {
        String proxyAddress = "localhost:9192";

        admin.createTopics(List.of(new NewTopic(TOPIC_1, 1, (short) 1))).all().get();

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers()).addNewFilter().withType("ProduceValidator").withConfig(Map.of("rules",
                List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                        Map.of("allowsNulls", true, "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true))))))
                .endFilter()
                .build().toYaml();

        try (var ignored = startProxy(config)) {
            try (var producer = getProducer(proxyAddress, 0)) {
                producer.send(new ProducerRecord<>(TOPIC_1, "my-key", SYNTACTICALLY_CORRECT_JSON)).get();
            }
        }
    }

}
