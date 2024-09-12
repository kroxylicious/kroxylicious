/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.filter.encryption.RecordEncryption;
import io.kroxylicious.filter.encryption.TemplateKekSelector;
import io.kroxylicious.filter.encryption.config.EncryptionVersion;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.ClientConfig;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * We want to ensure that we can always read data written to Kafka by older versions of
 * the filter. That means starting from binary injected into the upstream kafka cluster.
 * We used hard-coded binary data with real examples produced by a filter for a known KEK
 * id. This gives us confidence that the binary can be decoded. As opposed to constructing
 * the encrypted data as part of the test code.
 * We want to check that we can decode each encryption version and each supported cipher.
 */
@ExtendWith(KafkaClusterExtension.class)
class RecordEncryptionDeserializationCompatibilityIT {
    private static final String ENCRYPTED_AES_TOPIC = "aes-topic";
    private static final String PRE_EXISTING_AES_KEK_SECRET_BYTES = "FwailFttZJR2Jq43YPhwsKTtuTJQNnPPutrFu9uVPx4=";
    private static final String PRE_EXISTING_AES_KEK_UUID = "c32d1cef-9c60-4f86-b56b-281684c98ad0";
    private static final TestCase V2_VALUE_NOT_NULL = new TestCase(
            EncryptionVersion.V2,
            "value not null",
            ENCRYPTED_AES_TOPIC,
            new SerializedRecord(
                    List.of(new SerializedHeader("kroxylicious.io/encryption", "Ag==")),
                    "YQ==",
                    "AE6ADMHU4V/OFRDILMd8AsMtHO+cYE+GtWsoFoTJitA0Y8iY9RZBcQDxGqsIylR5ugQ57JYiqOOwLVPwm6X2t9iT/3VoUjJ/M8xFWpiczFIAT5ZR6To+WDLYniREKe+0/f6lRFsCb/MGEZhfVgkb3g=="
            ),
            new DeserializedRecord(List.of(), "a", "b")
    );

    // null values are serialized as-is with no encryption header, we need to forward null on to support tombstoning compacted topics
    private static final TestCase V2_VALUE_NULL = new TestCase(
            EncryptionVersion.V2,
            "value null",
            ENCRYPTED_AES_TOPIC,
            new SerializedRecord(
                    List.of(),
                    "YQ==",
                    null
            ),
            new DeserializedRecord(List.of(), "a", null)
    );
    private static final TestCase V2_OTHER_HEADERS = new TestCase(
            EncryptionVersion.V2,
            "other headers are preserved",
            ENCRYPTED_AES_TOPIC,
            new SerializedRecord(
                    List.of(
                            new SerializedHeader("kroxylicious.io/encryption", "Ag=="),
                            new SerializedHeader("x", "eQ==")
                    ),
                    "YQ==",
                    "AE6ADMNIqL7qk5zZDYBSp8MtHO+cYE+GtWsoFoTJitDZbCqfZvPsulMdhpO06acCIQ8unRSmTypxUAuaRyY5rZhrKP+WxohB5BUDHnMPztQAsUYlZQzgsNt3kbv4O7aPzm7Bh9FP2bl8c4fDXH33uA=="
            ),
            new DeserializedRecord(List.of(new DeserializedHeader("x", "y")), "a", "b")
    );

    record SerializedHeader(String key, @Nullable
    String valueBase64) {
        public byte[] valueBytes() {
            return valueBase64 == null ? null : Base64.getDecoder().decode(valueBase64);
        }

        public Header toKafkaHeader() {
            return new RecordHeader(key, valueBytes());
        }
    }

    record SerializedRecord(List<SerializedHeader> headers, @Nullable
    String keyBase64, @Nullable
    String valueBase64) {
        public byte[] keyBytes() {
            return keyBase64 == null ? null : Base64.getDecoder().decode(keyBase64);
        }

        public byte[] valueBytes() {
            return valueBase64 == null ? null : Base64.getDecoder().decode(valueBase64);
        }

        public Iterable<Header> kafkaHeaders() {
            return headers.stream().map(SerializedHeader::toKafkaHeader).collect(Collectors.toList());
        }
    }

    record DeserializedHeader(String name, @Nullable
    String value) {
        public Header kafkaHeader() {
            return new RecordHeader(name, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
        }
    }

    record DeserializedRecord(List<DeserializedHeader> headers, @Nullable
    String key, @Nullable
    String value) {
        public ProducerRecord<String, String> producerRecord(String topic) {
            return new ProducerRecord<>(
                    topic,
                    0,
                    key,
                    value,
                    headers.stream()
                           .map(DeserializedHeader::kafkaHeader)
                           .collect(
                                   Collectors.toList()
                           )
            );
        }
    }

    // for now v1 encryption is hardcoded and implicit, in future it should be a configurable aspect of
    // the filter.
    record TestCase(EncryptionVersion version, String name, String topic, SerializedRecord serializedRecord, DeserializedRecord expected) {
        public ProducerRecord<byte[], byte[]> producerRecord() {
            return new ProducerRecord<>(topic, 0, serializedRecord.keyBytes(), serializedRecord.valueBytes(), serializedRecord.kafkaHeaders());
        }
    }

    static Stream<Arguments> testCases() {
        return Stream.of(V2_VALUE_NOT_NULL, V2_OTHER_HEADERS, V2_VALUE_NULL).map(testCase -> Arguments.of(testCase.version, testCase.name, testCase));
    }

    @ParameterizedTest(name = "encryption version: {0} - {1}")
    @MethodSource("testCases")
    void testRecordsCanBeDeserialized(EncryptionVersion ignoredVersion, String ignoredName, TestCase testCase, KafkaCluster cluster, Producer<byte[], byte[]> producer)
                                                                                                                                                                        throws Exception {
        producer.send(testCase.producerRecord()).get(5, TimeUnit.SECONDS);
        try (var tester = kroxyliciousTester(proxy(cluster).addToFilters(buildEncryptionFilterDefinition()))) {
            Map<String, Object> config = Map.of(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            Consumer<String, String> consumer = tester.consumer(config);
            consumer.subscribe(List.of(ENCRYPTED_AES_TOPIC));
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(5));
            assertThat(poll.count()).isEqualTo(1);
            ConsumerRecord<String, String> onlyRecord = poll.iterator().next();
            assertExpected(testCase.expected, onlyRecord);
        }
    }

    private void assertExpected(DeserializedRecord expected, ConsumerRecord<String, String> onlyRecord) {
        List<DeserializedHeader> actual = StreamSupport.stream(onlyRecord.headers().spliterator(), false)
                                                       .map(
                                                               h -> new DeserializedHeader(
                                                                       h.key(),
                                                                       h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8)
                                                               )
                                                       )
                                                       .collect(Collectors.toList());
        assertThat(actual).isEqualTo(expected.headers);
        assertThat(onlyRecord.key()).isEqualTo(expected.key);
        assertThat(onlyRecord.value()).isEqualTo(expected.value);
    }

    @Disabled("convenience to generate serialized data and print it to console, run from IDE")
    @SuppressWarnings("java:S2699") // no assertions required
    @ParameterizedTest(name = "encryption version: {0} - {1}")
    @MethodSource("testCases")
    void generateData(
            EncryptionVersion ignoredVersion,
            String ignoredName,
            TestCase testCase,
            KafkaCluster cluster,
            @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "encryption") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest")
            Consumer<byte[], byte[]> consumer
    )
      throws ExecutionException,
      InterruptedException,
      TimeoutException {
        try (var tester = kroxyliciousTester(proxy(cluster).addToFilters(buildEncryptionFilterDefinition()))) {
            tester.producer().send(testCase.expected.producerRecord(ENCRYPTED_AES_TOPIC)).get(5, TimeUnit.SECONDS);
            consumer.subscribe(List.of(ENCRYPTED_AES_TOPIC));
            ConsumerRecords<byte[], byte[]> poll = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<byte[], byte[]> consumerRecord : poll) {
                print(consumerRecord);
            }
        }
    }

    @SuppressWarnings({ "checkstyle:RegexpSinglelineJava" })
    private static void print(ConsumerRecord<byte[], byte[]> consumerRecord) {
        for (Header header : consumerRecord.headers()) {
            byte[] value = header.value();
            System.out.println("header: " + header.key() + ":" + base64IfNotNull(value));
        }
        System.out.println("key: " + base64IfNotNull(consumerRecord.key()));
        System.out.println("value: " + base64IfNotNull(consumerRecord.value()));
    }

    private static String base64IfNotNull(byte[] value) {
        return value == null ? "null" : Base64.getEncoder().encodeToString(value);
    }

    private FilterDefinition buildEncryptionFilterDefinition() {
        byte[] kekSecret = Base64.getDecoder().decode(PRE_EXISTING_AES_KEK_SECRET_BYTES);
        // when we add support for more ciphers, we could add additional topics with the relevant key material for that cipher
        List<UnitTestingKmsService.Kek> existingKeks = List.of(
                new UnitTestingKmsService.Kek(PRE_EXISTING_AES_KEK_UUID, kekSecret, "AES", ENCRYPTED_AES_TOPIC)
        );
        return new FilterDefinitionBuilder(RecordEncryption.class.getSimpleName())
                                                                                  .withConfig("kms", UnitTestingKmsService.class.getSimpleName())
                                                                                  .withConfig("kmsConfig", new UnitTestingKmsService.Config(12, 128, existingKeks))
                                                                                  .withConfig("selector", TemplateKekSelector.class.getSimpleName())
                                                                                  .withConfig("selectorConfig", Map.of("template", "${topicName}"))
                                                                                  .build();
    }
}
