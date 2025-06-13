/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.filter.encryption.RecordEncryption;
import io.kroxylicious.filter.encryption.TemplateKekSelector;
import io.kroxylicious.filter.encryption.config.UnresolvedKeyPolicy;
import io.kroxylicious.filter.encryption.crypto.Encryption;
import io.kroxylicious.filter.encryption.crypto.EncryptionHeader;
import io.kroxylicious.filter.encryption.crypto.EncryptionResolver;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryTestKmsFacade;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.TestKmsFacadeInvocationContextProvider;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.test.tester.SimpleMetricAssert;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.ClientConfig;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;
import io.kroxylicious.testing.kafka.junit5ext.TopicConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.filter.encryption.RecordEncryptionMetrics.TOPIC_NAME;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThatCode;
import static org.assertj.core.api.InstanceOfAssertFactories.BYTE_ARRAY;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.contains;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(TestKmsFacadeInvocationContextProvider.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RecordEncryptionFilterIT {

    private static final String TEMPLATE_KEK_SELECTOR_PATTERN = "$(topicName)";
    private static final String HELLO_WORLD = "hello world";
    private static final String HELLO_SECRET = "hello secret";

    @TestTemplate
    void roundTripSingleRecord(KafkaCluster cluster, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade) throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);

        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

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

    @TestTemplate
    void roundTripTransactional(KafkaCluster cluster, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade) {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);

        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString()));
                var consumer = tester.consumer(Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"))) {
            producer.initTransactions();
            withTransaction(producer, transactionProducer -> {
                producer.send(new ProducerRecord<>(topic.name(), HELLO_WORLD)).get(5, TimeUnit.SECONDS);
            }).commitTransaction();
            consumer.subscribe(List.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(HELLO_WORLD);
        }
    }

    // check that records from aborted transaction are not exposed to read_committed clients
    @TestTemplate
    void roundTripTransactionalAbort(KafkaCluster cluster, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade) {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);

        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString()));
                var consumer = tester.consumer(Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"))) {
            producer.initTransactions();
            // send to the same partition to demonstrate a message appended to the same partition after the abort is made available
            String key = "key";
            withTransaction(producer, transactionProducer -> {
                producer.send(new ProducerRecord<>(topic.name(), key, "aborted message")).get(5, TimeUnit.SECONDS);
            }).abortTransaction();

            withTransaction(producer, transactionProducer -> {
                producer.send(new ProducerRecord<>(topic.name(), key, HELLO_WORLD)).get(5, TimeUnit.SECONDS);
            }).commitTransaction();

            consumer.subscribe(List.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(HELLO_WORLD);
        }
    }

    // check that records from uncommitted transaction are not exposed to read_committed clients
    @TestTemplate
    void roundTripTransactionalIsolation(KafkaCluster cluster, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade) {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);

        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString()));
                var consumer = tester.consumer(Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"))) {
            producer.initTransactions();

            withTransaction(producer, transactionProducer -> {
                transactionProducer.send(new ProducerRecord<>(topic.name(), "uncommitted message")).get(5, TimeUnit.SECONDS);
            });

            consumer.subscribe(List.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .isExhausted();
        }
    }

    <K, V> Producer<K, V> withTransaction(Producer<K, V> producer, ThrowingConsumer<Producer<K, V>> action) {
        producer.beginTransaction();
        try {
            action.accept(producer);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return producer;
    }

    @TestTemplate
    void roundTripManyRecordsFromDifferentProducers(KafkaCluster cluster, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade) throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);

        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

        try (var tester = kroxyliciousTester(builder);
                var producer1 = tester.producer();
                var producer2 = tester.producer();
                var consumer = tester.consumer()) {

            producer1.send(new ProducerRecord<>(topic.name(), HELLO_WORLD + 1));
            producer1.send(new ProducerRecord<>(topic.name(), HELLO_WORLD + 2));
            producer1.send(new ProducerRecord<>(topic.name(), HELLO_WORLD + 3)).get(5, TimeUnit.SECONDS);
            producer2.send(new ProducerRecord<>(topic.name(), HELLO_WORLD + 4));
            producer2.send(new ProducerRecord<>(topic.name(), HELLO_WORLD + 5)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .hasSize(5)
                    .extracting(ConsumerRecord::value)
                    .containsExactly(HELLO_WORLD + 1, HELLO_WORLD + 2, HELLO_WORLD + 3, HELLO_WORLD + 4, HELLO_WORLD + 5);
        }
    }

    // EDEKs can be configured with a time-based expiry. This gives us the nice property that after a KEK is rotated
    // in the external KMS a new EDEK will be generated using the new key.
    @TestTemplate
    void edekExpiry(KafkaCluster cluster, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade,
                    @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "rotation-test") KafkaConsumer<byte[], byte[]> directConsumer)
            throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);
        // 1 second is the current minimum configurable value
        Duration edekExpiry = Duration.ofSeconds(1);
        NamedFilterDefinitionBuilder filterDefinitionBuilder = new NamedFilterDefinitionBuilder("encrypt", RecordEncryption.class.getSimpleName());
        builder.addToFilterDefinitions(filterDefinitionBuilder
                .withConfig("kms", testKmsFacade.getKmsServiceClass().getSimpleName())
                .withConfig("kmsConfig", testKmsFacade.getKmsServiceConfig())
                .withConfig("experimental", Map.of("encryptionDekRefreshAfterWriteSeconds", edekExpiry.toSeconds()))
                .withConfig("selector", TemplateKekSelector.class.getSimpleName())
                .withConfig("selectorConfig", Map.of("template", TEMPLATE_KEK_SELECTOR_PATTERN))
                .build())
                .addToDefaultFilters(filterDefinitionBuilder.name());

        var message = "hello world";
        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer()) {
            // first message will cause an edek to be created (and cached)
            producer.send(new ProducerRecord<>(topic.name(), message)).get(5, TimeUnit.SECONDS);

            await().pollDelay(edekExpiry)
                    .pollInterval(Duration.ofMillis(100))
                    .atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> {
                        // async edek refresh will be trigger by the arrival of a message after edekExpiry
                        producer.send(new ProducerRecord<>(topic.name(), message)).get(5, TimeUnit.SECONDS);
                        assertMoreThanOneEdekUsed(topic, directConsumer);
                    });
        }
    }

    @Test
    void failedEncryptionRespondsWithError(KafkaCluster cluster, Topic topic, InMemoryTestKmsFacade testKmsFacade)
            throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);
        // 1 second is the current minimum configurable value
        Duration edekExpiry = Duration.ofSeconds(1);
        NamedFilterDefinitionBuilder filterDefinitionBuilder = new NamedFilterDefinitionBuilder("encrypt", RecordEncryption.class.getSimpleName());
        builder.addToFilterDefinitions(filterDefinitionBuilder
                .withConfig("kms", testKmsFacade.getKmsServiceClass().getSimpleName())
                .withConfig("kmsConfig", testKmsFacade.getKmsServiceConfig())
                .withConfig("experimental", Map.of(
                        "encryptionDekRefreshAfterWriteSeconds", edekExpiry.toSeconds(),
                        "maxEncryptionsPerDek", 1))
                .withConfig("selector", TemplateKekSelector.class.getSimpleName())
                .withConfig("selectorConfig", Map.of("template", TEMPLATE_KEK_SELECTOR_PATTERN))
                .build())
                .addToDefaultFilters(filterDefinitionBuilder.name());
        var messageOne = "hello world";
        var messageTwo = "hello world2";
        final String clientId = "producer-" + topic.name();
        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer(
                        Map.of("delivery.timeout.ms", 1500,
                                "linger.ms", 100,
                                "request.timeout.ms", 300,
                                "client.id", clientId))) {
            final AtomicReference<Exception> responseException = new AtomicReference<>();
            producer.send(new ProducerRecord<>(topic.name(), 0, "", messageOne), (metadata, exception) -> {
            });
            producer.send(new ProducerRecord<>(topic.name(), 0, "", messageTwo), (metadata, exception) -> responseException.set(exception));
            await().until(() -> responseException.get() != null);
            assertMetricHasCount(clientId, "connection-close-total", 0);
            assertThat(responseException).hasValueSatisfying(
                    actualException -> assertThat(actualException)
                            .isInstanceOf(TimeoutException.class)
                            .hasMessageStartingWith("Expiring 2 record(s) for"));
        }
    }

    private static void assertMetricHasCount(String clientId, String metricName, double expectedValue)
            throws MalformedObjectNameException, InstanceNotFoundException, IntrospectionException, ReflectionException, MBeanException, AttributeNotFoundException {
        final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        final ObjectName producerMetricsObjectName = ObjectName.getInstance("kafka.producer:type=producer-metrics,client-id=" + clientId);
        final MBeanInfo producerMetricsMbean = platformMBeanServer.getMBeanInfo(producerMetricsObjectName);
        final MBeanAttributeInfo attributeInfo = Arrays.stream(producerMetricsMbean.getAttributes())
                .filter(mBeanAttributeInfo -> metricName.equalsIgnoreCase(mBeanAttributeInfo.getName()))
                .findAny().orElseThrow(() -> new IllegalStateException(metricName + " metric not found"));
        final Object attribute = platformMBeanServer.getAttribute(producerMetricsObjectName, attributeInfo.getName());
        assertThat(attribute).describedAs("%s should metric %s with specified value", clientId, metricName).isEqualTo(expectedValue);
    }

    private static void assertMoreThanOneEdekUsed(Topic topic, KafkaConsumer<byte[], byte[]> directConsumer) {
        var partitions = List.of(new TopicPartition(topic.name(), 0));
        directConsumer.assign(partitions);
        directConsumer.seekToBeginning(partitions);
        ConsumerRecords<byte[], byte[]> records = directConsumer.poll(Duration.ofSeconds(2));
        Set<BytesEdek> edeks = StreamSupport.stream(records.spliterator(), false).map(kafkaRecord -> {
            List<byte[]> encryptionVersions = StreamSupport.stream(kafkaRecord.headers().headers(EncryptionHeader.ENCRYPTION_HEADER_NAME).spliterator(), false)
                    .map(Header::value).toList();
            assertThat(encryptionVersions).hasSize(1).singleElement(BYTE_ARRAY).hasSize(1);
            Encryption encryption = EncryptionResolver.ALL.fromSerializedId(encryptionVersions.getFirst()[0]);
            return encryption.wrapper().readSpecAndEdek(ByteBuffer.wrap(kafkaRecord.value()), BytesEdek.getSerde(), (cipherManager, o) -> o);
        }).collect(Collectors.toSet());
        assertThat(edeks).hasSizeGreaterThan(1);
    }

    // This ensures the decrypt-ability guarantee, post kek rotation
    @TestTemplate
    void decryptionAfterKekRotation(KafkaCluster cluster, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade) throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);
        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

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
        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

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
        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

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
        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

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
        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

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

    @TestTemplate
    void userCanChooseToRejectRecordsWhichWeCannotResolveKeysFor(KafkaCluster cluster, Topic encryptedTopic, Topic plainTopic, TestKmsFacade<?, ?, ?> testKmsFacade) {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(encryptedTopic.name());

        var builder = proxy(cluster);
        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.REJECT);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());
        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer(Map.of(ProducerConfig.RETRIES_CONFIG, 0, ProducerConfig.LINGER_MS_CONFIG, 1000));) {
            // we reject the entire produce request as a unit, this record should resolve to a key, but because plain topic does not resolve to a key both are rejected
            Future<RecordMetadata> sendA = producer.send(new ProducerRecord<>(encryptedTopic.name(), HELLO_SECRET));
            Future<RecordMetadata> sendB = producer.send(new ProducerRecord<>(plainTopic.name(), HELLO_WORLD));
            producer.flush();
            assertThat(sendA).failsWithin(5, TimeUnit.SECONDS).withThrowableThat()
                    .withCause(new InvalidRecordException("failed to resolve key for: [" + plainTopic.name() + "]"));
            assertThat(sendB).failsWithin(5, TimeUnit.SECONDS).withThrowableThat()
                    .withCause(new InvalidRecordException("failed to resolve key for: [" + plainTopic.name() + "]"));
        }
    }

    /**
     * Test that ensures that the record offsets returned by the broker are faithfully relayed to the client.
     * @param cluster underlying kafka cluster
     * @param compactedTopic topic configured for compaction.
     * @param directConsumer consumer connected directly to the underlying kafka cluster
     * @param testKmsFacade kms facade
     * @throws Exception exception
     */
    @TestTemplate
    @SuppressWarnings("java:S2925")
    void offsetFidelity(@BrokerConfig(name = "log.cleaner.backoff.ms", value = "50") KafkaCluster cluster,
                        @TopicConfig(name = "segment.ms", value = "125") @TopicConfig(name = "cleanup.policy", value = "compact") Topic compactedTopic,
                        Consumer<String, String> directConsumer,
                        TestKmsFacade<?, ?, ?> testKmsFacade)
            throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(compactedTopic.name());

        var builder = proxy(cluster);

        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

        try (var tester = kroxyliciousTester(builder);
                var proxyProducer = tester.producer();
                var proxyConsumer = tester.consumer()) {

            proxyProducer.send(new ProducerRecord<>(compactedTopic.name(), "a", "a1"));
            // Send two messages for key "b", the first will be eligible for compaction
            proxyProducer.send(new ProducerRecord<>(compactedTopic.name(), "b", "b1"));
            proxyProducer.send(new ProducerRecord<>(compactedTopic.name(), "b", "b2")).get(5, TimeUnit.SECONDS);

            // Sleep for segment.ms so that the broker will begin a new segment when the next produce is received.
            // The records in the first segment will become eligible for compaction.
            Thread.sleep(125);
            proxyProducer.send(new ProducerRecord<>(compactedTopic.name(), "c", "c1")).get(5, TimeUnit.SECONDS);

            // Wait until the topic compaction has coalesced the two b records into one.
            // This will result in a gap in the offsets.
            var directlyReadRecords = await().atMost(Duration.ofSeconds(30))
                    .pollDelay(Duration.ofSeconds(1))
                    .until(() -> consumeAll(compactedTopic.name(), 0, directConsumer).map(RecordEncryptionFilterIT::stringifyRecordKeyOffset).toList(),
                            contains("a:0", "b:2", "c:3"));

            var proxyReadRecords = consumeAll(compactedTopic.name(), 0, proxyConsumer).map(RecordEncryptionFilterIT::stringifyRecordKeyOffset).toList();

            assertThat(proxyReadRecords).isEqualTo(directlyReadRecords);
        }
    }

    private static <K, V> String stringifyRecordKeyOffset(ConsumerRecord<K, V> rec) {
        return "%s:%d".formatted(rec.key(), rec.offset());
    }

    @NonNull
    private Stream<ConsumerRecord<String, String>> consumeAll(String topicName, int partition, Consumer<String, String> consumer) {
        var partitions = List.of(new TopicPartition(topicName, partition));
        try {
            consumer.assign(partitions);
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            consumer.seekToBeginning(partitions);
            ConsumerRecords<String, String> last;
            do {
                last = consumer.poll(Duration.ofSeconds(1));
                last.forEach(records::add);
            } while (!last.isEmpty());
            return records.stream();
        }
        finally {
            consumer.assign(List.of());
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
        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

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

    @TestTemplate
    void checkMetricsIncrementedOnEncryptedAndPlainTopic(KafkaCluster cluster, Topic encryptedTopic, Topic plainTopic, TestKmsFacade<?, ?, ?> testKmsFacade) {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(encryptedTopic.name());

        var builder = proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        NamedFilterDefinition namedFilterDefinition = buildEncryptionFilterDefinition(testKmsFacade, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
        builder.addToFilterDefinitions(namedFilterDefinition);
        builder.addToDefaultFilters(namedFilterDefinition.name());

        try (var tester = kroxyliciousTester(builder);
                var managementClient = tester.getManagementClient();
                var producer = tester.producer(Map.of(ProducerConfig.LINGER_MS_CONFIG, 1000, ProducerConfig.BATCH_SIZE_CONFIG, 2));
                var consumer = tester.consumer()) {

            producer.send(new ProducerRecord<>(encryptedTopic.name(), HELLO_SECRET));
            producer.send(new ProducerRecord<>(plainTopic.name(), HELLO_WORLD));
            producer.flush();

            var metricList = managementClient.scrapeMetrics();

            System.out.println(metricList);

            SimpleMetricAssert.assertThat(metricList)
                    .filterByName("kroxylicious_not_encrypted_records_total")
                    .filterByTag(TOPIC_NAME, plainTopic.name())
                    .singleElement()
                    .value()
                    .isGreaterThanOrEqualTo(1.0);

            SimpleMetricAssert.assertThat(metricList)
                    .filterByName("kroxylicious_encrypted_records_total")
                    .filterByTag(TOPIC_NAME, encryptedTopic.name())
                    .singleElement()
                    .value()
                    .isGreaterThanOrEqualTo(1.0);
        }
    }

    private NamedFilterDefinition buildEncryptionFilterDefinition(TestKmsFacade<?, ?, ?> testKmsFacade, UnresolvedKeyPolicy unresolvedKeyPolicy) {
        return new NamedFilterDefinitionBuilder("filter-1", RecordEncryption.class.getSimpleName())
                .withConfig("kms", testKmsFacade.getKmsServiceClass().getSimpleName())
                .withConfig("kmsConfig", testKmsFacade.getKmsServiceConfig())
                .withConfig("selector", TemplateKekSelector.class.getSimpleName())
                .withConfig("selectorConfig", Map.of("template", TEMPLATE_KEK_SELECTOR_PATTERN))
                .withConfig("unresolvedKeyPolicy", unresolvedKeyPolicy)
                .build();
    }

}
