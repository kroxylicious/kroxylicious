/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.filter.encryption.EnvelopeEncryption;
import io.kroxylicious.filter.encryption.TemplateKekSelector;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.KekId;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.ClientConfig;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThatCode;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(EnvelopeEncryptionTestInvocationContextProvider.class)
class EnvelopeEncryptionFilterIT {

    private static final String TEMPLATE_KEK_SELECTOR_PATTERN = "${topicName}";
    private static final String HELLO_WORLD = "hello world";
    private static final String HELLO_SECRET = "hello secret";

    @TestTemplate
    void roundTrip(KafkaCluster cluster, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade) throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);

        builder.addToFilters(buildEncryptionFilterDefinition(testKmsFacade));

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer();
                var consumer = tester.consumer()) {

            producer.send(new ProducerRecord<>(topic.name(), HELLO_WORLD)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(HELLO_WORLD);
        }
    }

    // This ensures the decrypt-ability guarantee, post kek rotation
    @TestTemplate
    void decryptionAfterKekRotation(KafkaCluster cluster, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade) throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);
        builder.addToFilters(buildEncryptionFilterDefinition(testKmsFacade));

        var messageBeforeKeyRotation = "hello world, old key";
        var messageAfterKeyRotation = "hello world, new key";
        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer()) {
            producer.send(new ProducerRecord<>(topic.name(), messageBeforeKeyRotation)).get(5, TimeUnit.SECONDS);

            // Now do the Kek rotation
            testKekManager.rotateKek(topic.name());

            producer.send(new ProducerRecord<>(topic.name(), messageAfterKeyRotation)).get(5, TimeUnit.SECONDS);

            try (var consumer = tester.consumer()) {
                consumer.subscribe(List.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(2));
                assertThat(records.iterator())
                        .toIterable()
                        .extracting(ConsumerRecord::value)
                        .containsExactly(messageBeforeKeyRotation, messageAfterKeyRotation);
            }
        }
    }

    @TestTemplate
    void topicRecordsAreUnreadableOnServer(KafkaCluster cluster, Topic topic, KafkaConsumer<String, String> directConsumer, TestKmsFacade<?, ?, ?> testKmsFacade)
            throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);
        builder.addToFilters(buildEncryptionFilterDefinition(testKmsFacade));

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer()) {

            var message = HELLO_WORLD;
            producer.send(new ProducerRecord<>(topic.name(), message)).get(5, TimeUnit.SECONDS);

            var tps = List.of(new TopicPartition(topic.name(), 0));
            directConsumer.assign(tps);
            directConsumer.seekToBeginning(tps);
            var records = directConsumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isNotEqualTo(message);
        }
    }

    @TestTemplate
    void unencryptedRecordsConsumable(KafkaCluster cluster, KafkaProducer<String, String> directProducer, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade)
            throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);
        builder.addToFilters(buildEncryptionFilterDefinition(testKmsFacade));

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer();
                var consumer = tester.consumer()) {

            // messages produced via Kroxylicious will be encrypted
            producer.send(new ProducerRecord<>(topic.name(), HELLO_SECRET)).get(5, TimeUnit.SECONDS);

            // messages produced direct will be plain
            directProducer.send(new ProducerRecord<>(topic.name(), HELLO_WORLD)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator()).toIterable()
                    .hasSize(2)
                    .map(ConsumerRecord::value)
                    .contains(HELLO_SECRET, HELLO_WORLD);
        }
    }

    @TestTemplate
    void nullValueRecordProducedAndConsumedSuccessfully(KafkaCluster cluster,
                                                        @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "test") Consumer<String, String> directConsumer,
                                                        Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade)
            throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);
        builder.addToFilters(buildEncryptionFilterDefinition(testKmsFacade));

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer();
                var consumer = tester.consumer()) {

            String message = null;
            producer.send(new ProducerRecord<>(topic.name(), message)).get(5, TimeUnit.SECONDS);

            assertOnlyValueInTopicHasNullValue(consumer, topic.name());
            // test that the null-value is preserved in Kafka to keep compaction tombstoning working
            assertOnlyValueInTopicHasNullValue(directConsumer, topic.name());
        }
    }

    private static void assertOnlyValueInTopicHasNullValue(Consumer<String, String> consumer, String topic) {
        consumer.subscribe(List.of(topic));
        var records = consumer.poll(Duration.ofSeconds(2));
        assertThat(records.iterator())
                .toIterable()
                .singleElement()
                .extracting(ConsumerRecord::value)
                .isNull();
    }

    @TestTemplate
    void produceAndConsumeEncryptedAndPlainTopicsAtSameTime(KafkaCluster cluster, Topic encryptedTopic, Topic plainTopic, TestKmsFacade<?, ?, ?> testKmsFacade)
            throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(encryptedTopic.name());

        var builder = proxy(cluster);
        builder.addToFilters(buildEncryptionFilterDefinition(testKmsFacade));

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer(Map.of(ProducerConfig.LINGER_MS_CONFIG, 1000, ProducerConfig.BATCH_SIZE_CONFIG, 2));
                var consumer = tester.consumer()) {

            producer.send(new ProducerRecord<>(encryptedTopic.name(), HELLO_SECRET));
            producer.send(new ProducerRecord<>(plainTopic.name(), HELLO_WORLD));
            producer.flush();

            consumer.subscribe(List.of(encryptedTopic.name(), plainTopic.name()));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .extracting(ConsumerRecord::value)
                    .contains(HELLO_SECRET, HELLO_WORLD);
        }
    }

    // TODO express this test as a unit test and consider doing away with the test as the IT level.
    @TestTemplate
    void shouldGenerateOneDek(KafkaCluster cluster, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade) throws Exception {
        assumeThatCode(testKmsFacade::getKms).doesNotThrowAnyException();
        assertThat(testKmsFacade.getKms()).isInstanceOf(InMemoryKms.class);

        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);
        builder.addToFilters(buildEncryptionFilterDefinition(testKmsFacade));

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer(Map.of(ProducerConfig.LINGER_MS_CONFIG, 0));
                var consumer = tester.consumer()) {

            producer.send(new ProducerRecord<>(topic.name(), HELLO_WORLD)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(HELLO_WORLD);

            // Send two batches to the same topic
            var message = "hello world #2";
            producer.send(new ProducerRecord<>(topic.name(), message)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic.name()));
            records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(message);
        }

        assertThat(testKmsFacade.getKms())
                .asInstanceOf(InstanceOfAssertFactories.type(InMemoryKms.class))
                .extracting(InMemoryKms::numDeksGenerated).isEqualTo(1);

    }

    private FilterDefinition buildEncryptionFilterDefinition(TestKmsFacade<?, ?, ?> testKmsFacade) {
        return new FilterDefinitionBuilder(EnvelopeEncryption.class.getSimpleName())
                .withConfig("kms", testKmsFacade.getKmsServiceClass().getSimpleName())
                .withConfig("kmsConfig", testKmsFacade.getKmsServiceConfig())
                .withConfig("selector", TemplateKekSelector.class.getSimpleName())
                .withConfig("selectorConfig", Map.of("template", TEMPLATE_KEK_SELECTOR_PATTERN))
                .build();
    }

}