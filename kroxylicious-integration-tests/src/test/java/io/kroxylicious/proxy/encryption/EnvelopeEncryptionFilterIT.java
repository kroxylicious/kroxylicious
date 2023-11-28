/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.filter.encryption.TemplateKekSelector;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.DEFAULT_VIRTUAL_CLUSTER;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
class EnvelopeEncryptionFilterIT {

    @Test
    void roundTrip(KafkaCluster cluster) throws Exception {
        var kmsId = UUID.randomUUID();
        var kms = provisionKms(kmsId);
        generateKekWithAlias(kms);

        var builder = proxy(cluster);

        builder.addToFilters(encryptionFilterDefinition(kmsId));

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin();
                var producer = tester.producer();
                var consumer = tester.consumer()) {

            String topic = tester.createTopic(DEFAULT_VIRTUAL_CLUSTER);

            await().atMost(Duration.ofSeconds(5)).until(() -> admin.listTopics().namesToListings().get(),
                    n -> n.containsKey(topic));

            var message = "hello world";
            producer.send(new ProducerRecord<>(topic, message)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(message);
        }
    }

    // This ensures the decrypt-ability guarantee, post kek rotation
    @Test
    void decryptionAfterKekRotation(KafkaCluster cluster, Admin admin) throws Exception {
        var kmsId = UUID.randomUUID();
        var kms = provisionKms(kmsId);
        generateKekWithAlias(kms);

        var topicName = UUID.randomUUID().toString();
        admin.createTopics(List.of(new NewTopic(topicName, Optional.empty(), Optional.empty()))).all().get(5, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(5)).until(() -> admin.listTopics().namesToListings().get(),
                n -> n.containsKey(topicName));

        var builder = proxy(cluster);
        builder.addToFilters(encryptionFilterDefinition(kmsId));

        var messageBeforeKeyRotation = "hello world, old key";
        var messageAfterKeyRotation = "hello world, new key";
        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer()) {
            producer.send(new ProducerRecord<>(topicName, messageBeforeKeyRotation)).get(5, TimeUnit.SECONDS);
        }

        // Now do the Kek rotation
        generateKekWithAlias(kms);

        builder.addToFilters(0, encryptionFilterDefinition(kmsId));

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer();
                var consumer = tester.consumer()) {

            producer.send(new ProducerRecord<>(topicName, messageAfterKeyRotation)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topicName));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .extracting(ConsumerRecord::value)
                    .containsExactly(messageBeforeKeyRotation, messageAfterKeyRotation);
        }

        IntegrationTestingKmsService.delete(kmsId.toString());
    }

    @NonNull
    private InMemoryKms provisionKms(UUID kmsId) {
        return IntegrationTestingKmsService.newInstance().buildKms(new IntegrationTestingKmsService.Config(kmsId.toString()));
    }

    private UUID generateKekWithAlias(InMemoryKms kms) {
        var kekId = kms.generateKey();
        kms.createAlias(kekId, "all");
        return kekId;
    }

    @Test
    void topicRecordsAreUnreadableOnServer(KafkaCluster cluster, KafkaConsumer<String, String> directConsumer) throws Exception {
        var kmsId = UUID.randomUUID();
        var kms = provisionKms(kmsId);
        generateKekWithAlias(kms);

        var builder = proxy(cluster);
        builder.addToFilters(encryptionFilterDefinition(kmsId));

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin();
                var producer = tester.producer()) {

            String topic = tester.createTopic(DEFAULT_VIRTUAL_CLUSTER);

            await().atMost(Duration.ofSeconds(5)).until(() -> admin.listTopics().namesToListings().get(),
                    n -> n.containsKey(topic));

            var message = "hello world";
            producer.send(new ProducerRecord<>(topic, message)).get(5, TimeUnit.SECONDS);

            var tps = List.of(new TopicPartition(topic, 0));
            directConsumer.assign(tps);
            directConsumer.seekToBeginning(tps);
            var records = directConsumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isNotEqualTo(message);
        }

        IntegrationTestingKmsService.delete(kmsId.toString());
    }

    @Test
    void unencryptedRecordsConsumable(KafkaCluster cluster, KafkaProducer<String, String> directProducer) throws Exception {
        var kmsId = UUID.randomUUID();
        var kms = provisionKms(kmsId);
        generateKekWithAlias(kms);

        var builder = proxy(cluster);

        builder.addToFilters(encryptionFilterDefinition(kmsId));

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin();
                var producer = tester.producer();
                var consumer = tester.consumer()) {

            String topic = tester.createTopic(DEFAULT_VIRTUAL_CLUSTER);

            await().atMost(Duration.ofSeconds(5)).until(() -> admin.listTopics().namesToListings().get(),
                    n -> n.containsKey(topic));

            // messages produced via Kroxylicious will be encrypted
            var message = "hello encrypted world";
            producer.send(new ProducerRecord<>(topic, message)).get(5, TimeUnit.SECONDS);

            // messages produced direct will be plain
            var plainMessage = "hello plain world";
            directProducer.send(new ProducerRecord<>(topic, plainMessage)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator()).toIterable()
                    .hasSize(2)
                    .map(ConsumerRecord::value)
                    .containsExactly(message, plainMessage);
        }

        IntegrationTestingKmsService.delete(kmsId.toString());
    }

    @Test
    @Disabled("InBandKeyManger doesn't handle nulls")
    void nullValueRecordProducedAndConsumedSuccessfully(KafkaCluster cluster) throws Exception {
        var kmsId = UUID.randomUUID();
        var kms = provisionKms(kmsId);
        generateKekWithAlias(kms);

        var builder = proxy(cluster);

        builder.addToFilters(encryptionFilterDefinition(kmsId));

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin();
                var producer = tester.producer();
                var consumer = tester.consumer()) {

            String topic = tester.createTopic(DEFAULT_VIRTUAL_CLUSTER);

            await().atMost(Duration.ofSeconds(5)).until(() -> admin.listTopics().namesToListings().get(),
                    n -> n.containsKey(topic));

            String message = null;
            producer.send(new ProducerRecord<>(topic, message)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isNull();
        }

        IntegrationTestingKmsService.delete(kmsId.toString());
    }

    @Test
    void produceAndConsumeEncryptedAndPlainTopicsAtSameTime(KafkaCluster cluster, Admin admin) throws Exception {
        var kmsId = UUID.randomUUID();
        var kms = provisionKms(kmsId);
        var keyId = generateKekWithAlias(kms);

        var encryptedTopic = UUID.randomUUID().toString();
        var plainTopic = UUID.randomUUID().toString();

        var topics = Stream.of(encryptedTopic, plainTopic)
                .map(t -> new NewTopic(t, Optional.empty(), Optional.empty()))
                .toList();
        admin.createTopics(topics).all().get(5, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(5)).until(() -> admin.listTopics().namesToListings().get(),
                n -> n.containsKey(encryptedTopic) && n.containsKey(plainTopic));

        var builder = proxy(cluster);

        builder.addToFilters(encryptionFilterDefinition(kmsId));

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer(Map.of(ProducerConfig.LINGER_MS_CONFIG, 1000, ProducerConfig.BATCH_SIZE_CONFIG, 2));
                var consumer = tester.consumer()) {

            var secretMessage = "hello secret";
            var plainMessage = "hello world";

            producer.send(new ProducerRecord<>(encryptedTopic, secretMessage));
            producer.send(new ProducerRecord<>(plainTopic, plainMessage));
            producer.flush();

            consumer.subscribe(List.of(encryptedTopic, plainTopic));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .extracting(ConsumerRecord::value)
                    .contains(secretMessage, plainMessage);
        }

        IntegrationTestingKmsService.delete(kmsId.toString());
    }

    @Test
    void shouldGenerateOneDek(KafkaCluster cluster) throws Exception {
        var kmsId = UUID.randomUUID();
        var kms = provisionKms(kmsId);
        var keyId = generateKekWithAlias(kms);

        var builder = proxy(cluster);

        builder.addToFilters(encryptionFilterDefinition(kmsId));

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin();
                var producer = tester.producer(Map.of(ProducerConfig.LINGER_MS_CONFIG, 0));
                var consumer = tester.consumer()) {

            String topic = tester.createTopic(DEFAULT_VIRTUAL_CLUSTER);

            await().atMost(Duration.ofSeconds(5)).until(() -> admin.listTopics().namesToListings().get(),
                    n -> n.containsKey(topic));

            var message = "hello world";
            producer.send(new ProducerRecord<>(topic, message)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(message);

            // Send two batches to the same topic
            message = "hello world #2";
            producer.send(new ProducerRecord<>(topic, message)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic));
            records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(message);
        }

        // We expect that only one DEK was generated
        assertThat(kms.numDeksGenerated()).isEqualTo(1);

        IntegrationTestingKmsService.delete(kmsId.toString());
    }

    /*
     *
     * further IT ideas:
     * fetching from > 1 topics (mixed encryption/plain case)
     * exploratory test examining what the client will see/do when decryption fails - looking to verify
     * - behaviour is reasonable
     * - the user has a chance to understand what's wrong.
     * behaviour when KMS throws a KmsException a) during produce, b) during fetch
     */
    private FilterDefinition encryptionFilterDefinition(UUID kmsId) {
        return new FilterDefinitionBuilder("EnvelopeEncryption")
                .withConfig("kms", IntegrationTestingKmsService.class.getSimpleName())
                .withConfig("kmsConfig", Map.of("name", kmsId.toString()))
                .withConfig("selector", TemplateKekSelector.class.getSimpleName())
                .withConfig("selectorConfig", Map.of("template", "all"))
                .build();

    }
}
