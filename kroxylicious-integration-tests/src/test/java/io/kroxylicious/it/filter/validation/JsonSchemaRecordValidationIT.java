/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.filter.validation;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.assertj.core.api.iterable.ThrowingExtractor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.apicurio.registry.rest.client.models.CreateArtifact;
import io.apicurio.registry.rest.client.models.CreateVersion;
import io.apicurio.registry.rest.client.models.VersionContent;
import io.apicurio.registry.rest.client.models.VersionMetaData;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.jsonschema.JsonSchemaSerde;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;

import io.kroxylicious.filter.validation.RecordValidation;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@EnabledIf(value = "isDockerAvailable", disabledReason = "docker unavailable")
class JsonSchemaRecordValidationIT extends RecordValidationBaseIT {

    private static final String JSON_SCHEMA_TOPIC_1 = """
            {
              "$id": "https://example.com/person.schema.json",
              "$schema": "http://json-schema.org/draft-07/schema#",
              "title": "Person",
              "type": "object",
              "properties": {
                "firstName": {
                  "type": "string",
                  "description": "The person's first name."
                },
                "lastName": {
                  "type": "string",
                  "description": "The person's last name."
                },
                "age": {
                  "description": "Age in years which must be equal to or greater than zero.",
                  "type": "integer",
                  "minimum": 0
                }
              }
            }
            """;

    private static final String JSON_SCHEMA_TOPIC_2 = """
            {
              "$id": "https://example.com/person2.schema.json",
              "$schema": "http://json-schema.org/draft-07/schema#",
              "title": "Person2",
              "type": "object",
              "properties": {
                "firstName": {
                  "type": "string",
                  "description": "The person's first name."
                },
                "lastName": {
                  "type": "string",
                  "description": "The person's last name."
                },
                "age": {
                  "description": "Age in years which must be equal to or greater than zero.",
                  "type": "integer",
                  "minimum": 0
                },
                "email": {
                  "type": "string",
                  "description": "The person's email address."
                }
              }
            }
            """;

    private static final String JSON_MESSAGE = """
            {"firstName":"json1","lastName":"json2"}""";
    private static final String INVALID_AGE_MESSAGE = """
            {"firstName":"json1","lastName":"json2","age":-3}""";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final PersonBean PERSON_BEAN = new PersonBean("john", "smith", 23);

    private static String apicurioRegistryUrl;
    private static String firstArtifactId;
    private static int firstContentId;
    private static int secondContentId;
    private static GenericContainer<?> registryContainer;

    @BeforeAll
    static void init() {
        registryContainer = startRegistryContainer();
        apicurioRegistryUrl = registryUrl(registryContainer);

        var client = registryClient(apicurioRegistryUrl);

        // Create first artifact with JSON_SCHEMA_TOPIC_1
        CreateArtifact createFirstArtifact = new CreateArtifact();
        createFirstArtifact.setArtifactType("JSON");
        CreateVersion firstVersion = new CreateVersion();
        VersionContent firstContent = new VersionContent();
        firstContent.setContent(JSON_SCHEMA_TOPIC_1);
        firstContent.setContentType("application/json");
        firstVersion.setContent(firstContent);
        createFirstArtifact.setFirstVersion(firstVersion);

        // Create second artifact with JSON_SCHEMA_TOPIC_2 (different schema to get different contentId)
        CreateArtifact createSecondArtifact = new CreateArtifact();
        createSecondArtifact.setArtifactType("JSON");
        CreateVersion secondVersion = new CreateVersion();
        VersionContent secondContent = new VersionContent();
        secondContent.setContent(JSON_SCHEMA_TOPIC_2);
        secondContent.setContentType("application/json");
        secondVersion.setContent(secondContent);
        createSecondArtifact.setFirstVersion(secondVersion);

        VersionMetaData firstArtifact = client.groups().byGroupId("default").artifacts().post(createFirstArtifact).getVersion();
        VersionMetaData secondArtifact = client.groups().byGroupId("default").artifacts().post(createSecondArtifact).getVersion();
        firstArtifactId = firstArtifact.getArtifactId();
        firstContentId = firstArtifact.getContentId().intValue();
        secondContentId = secondArtifact.getContentId().intValue();
    }

    @Test
    void shouldAcceptValidJsonInProduceRequest(KafkaCluster cluster, Topic topic) {
        var config = createContentIdRecordValidationConfig(cluster, topic, "valueRule", firstContentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", JSON_MESSAGE)))
                    .succeedsWithin(Duration.ofSeconds(10));

            var records = consumeAll(tester, topic);

            assertSingleRecordInTopicHasProperty(records, topic, ConsumerRecord::value, JSON_MESSAGE);
        }
    }

    @Test
    void invalidAgeProduceRejectedUsingTopicNames(KafkaCluster cluster, Topic topic1, Topic topic2) {
        // Topic 2 has schema validation, invalid data cannot be sent.
        var config = createContentIdRecordValidationConfig(cluster, topic2, "valueRule", firstContentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            // Topic 2 has schema validation defined, invalid data cannot be produced.
            var invalid = producer.send(new ProducerRecord<>(topic2.name(), "my-key", INVALID_AGE_MESSAGE));
            assertThatFutureFails(invalid, InvalidRecordException.class, "$.age: must have a minimum value of 0");

            // Topic 1 has no schema validation, invalid data is produced.
            var accepted = producer.send(new ProducerRecord<>(topic1.name(), "my-key", INVALID_AGE_MESSAGE));
            assertThatFutureSucceeds(accepted);

            var records = consumeAll(tester, topic1);

            assertSingleRecordInTopicHasProperty(records, topic1, ConsumerRecord::value, INVALID_AGE_MESSAGE);

        }
    }

    @Test
    void nonExistentSchema(KafkaCluster cluster, Topic topic) {
        var config = createContentIdRecordValidationConfig(cluster, topic, "valueRule", 3);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(topic.name(), "my-key", JSON_MESSAGE));
            assertThatFutureFails(invalid, InvalidRecordException.class, "No content with ID '3' was found.");
        }
    }

    record PersonBean(String firstName, String lastName, int age) {}

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void clientSideUsesValueSchemasToo(boolean schemaIdInHeader, KafkaCluster cluster, Topic topic) {
        var config = createContentIdRecordValidationConfig(cluster, topic, "valueRule", firstContentId);

        var keySerde = new Serdes.StringSerde();
        var producerValueSerde = createJsonSchemaProducerSerde(schemaIdInHeader, false);
        var consumerValueSerde = createJsonSchemaConsumerSerde(schemaIdInHeader, false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, producerValueSerde, Map.of());
                var consumer = consumeFromEarliestOffsets(tester, keySerde, consumerValueSerde)) {
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", PERSON_BEAN)))
                    .succeedsWithin(Duration.ofSeconds(10));
            consumer.subscribe(Set.of(topic.name()));

            var records = consumer.poll(Duration.ofSeconds(10));

            // note that when the schemaid is in the value, there's no type information so the deserializer will give a JsonNode.
            var expected = schemaIdInHeader ? PERSON_BEAN : OBJECT_MAPPER.valueToTree(PERSON_BEAN);
            assertSingleRecordInTopicHasProperty(records, topic, ConsumerRecord::value, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void clientSideUsesKeySchemasToo(boolean schemaIdInHeader, KafkaCluster cluster, Topic topic) {
        var config = createContentIdRecordValidationConfig(cluster, topic, "keyRule", firstContentId);

        boolean isKey = true;
        var producerKeySerde = createJsonSchemaProducerSerde(schemaIdInHeader, isKey);
        var consumerKeySerde = createJsonSchemaConsumerSerde(schemaIdInHeader, isKey);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerKeySerde, new Serdes.StringSerde(), Map.of());
                var consumer = consumeFromEarliestOffsets(tester, consumerKeySerde, new Serdes.StringSerde())) {
            assertThat(producer.send(new ProducerRecord<>(topic.name(), PERSON_BEAN, "my-value")))
                    .succeedsWithin(Duration.ofSeconds(10));
            consumer.subscribe(Set.of(topic.name()));

            var records = consumer.poll(Duration.ofSeconds(10));

            // note that when the schemaid is in the value, there's no type information so the deserializer will give a JsonNode.
            var expected = schemaIdInHeader ? PERSON_BEAN : OBJECT_MAPPER.valueToTree(PERSON_BEAN);
            assertSingleRecordInTopicHasProperty(records, topic, ConsumerRecord::key, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void detectsClientProducingWithWrongValueSchemaId(boolean schemaIdInHeader, KafkaCluster cluster, Topic topic) {
        var config = createContentIdRecordValidationConfig(cluster, topic, "valueRule", secondContentId);

        var keySerde = new Serdes.StringSerde();
        var valueSerde = createJsonSchemaProducerSerde(schemaIdInHeader, false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, valueSerde, Map.of())) {
            var invalid = producer.send(new ProducerRecord<>(topic.name(), "my-key", PERSON_BEAN));
            assertThatFutureFails(invalid, InvalidRecordException.class, "Unexpected schema id in record (1), expecting 2");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void detectsClientProducingWithWrongKeySchemaId(boolean schemaIdInHeader, KafkaCluster cluster, Topic topic) {
        var config = createContentIdRecordValidationConfig(cluster, topic, "keyRule", secondContentId);

        var valueSerde = new Serdes.StringSerde();
        var keySerde = createJsonSchemaProducerSerde(schemaIdInHeader, true);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, valueSerde, Map.of())) {
            var invalid = producer.send(new ProducerRecord<>(topic.name(), PERSON_BEAN, "my-key"));
            assertThatFutureFails(invalid, InvalidRecordException.class, "Unexpected schema id in record (1), expecting 2");
        }
    }

    private static <T, S, U> void assertSingleRecordInTopicHasProperty(ConsumerRecords<T, S> records, Topic topic,
                                                                       ThrowingExtractor<ConsumerRecord<T, S>, U, RuntimeException> property, U expected) {
        assertThat(records.records(topic.name()))
                .hasSize(1)
                .map(property)
                .containsExactly(expected);
    }

    /** Helper methods to create Serdes with JsonSchemaSerde configured for Apicurio Registry v3.
     * These methods are configured to use v3 wire format (Confluent-compatible 4-byte content IDs).
     * The default IdHandler (Default4ByteIdHandler) is used for 4-byte content IDs in the wire format.
     * ContentId strategy is used as the default for Apicurio Registry v3.
     **/
    private static @NonNull JsonSchemaSerde<Object> createJsonSchemaConsumerSerde(boolean schemaIdInHeader, boolean isKey) {
        var consumerKeySerde = new JsonSchemaSerde<>();
        consumerKeySerde.configure(Map.of(
                SerdeConfig.EXPLICIT_ARTIFACT_ID, firstArtifactId,
                SerdeConfig.EXPLICIT_ARTIFACT_VERSION, "1",
                KafkaSerdeConfig.ENABLE_HEADERS, schemaIdInHeader,
                SerdeConfig.REGISTRY_URL, apicurioRegistryUrl), isKey);
        return consumerKeySerde;
    }

    private static @NonNull JsonSchemaSerde<PersonBean> createJsonSchemaProducerSerde(boolean schemaIdInHeader, boolean isKey) {
        var producerKeySerde = new JsonSchemaSerde<PersonBean>();
        producerKeySerde.configure(Map.of(
                SerdeConfig.REGISTRY_URL, apicurioRegistryUrl,
                SerdeConfig.EXPLICIT_ARTIFACT_ID, firstArtifactId,
                SerdeConfig.EXPLICIT_ARTIFACT_VERSION, "1",
                KafkaSerdeConfig.ENABLE_HEADERS, schemaIdInHeader), isKey);
        return producerKeySerde;
    }

    @Test
    void v3WireFormatValidationWorks(KafkaCluster cluster, Topic topic) {
        // Explicitly configure V3 wire format (4-byte content IDs, Confluent-compatible)
        var config = createContentIdRecordValidationConfigWithWireFormat(cluster, topic, "valueRule", firstContentId, "V3");

        var keySerde = new Serdes.StringSerde();
        var producerValueSerde = createJsonSchemaProducerSerde(false, false);
        var consumerValueSerde = createJsonSchemaConsumerSerde(false, false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, producerValueSerde, Map.of());
                var consumer = consumeFromEarliestOffsets(tester, keySerde, consumerValueSerde)) {
            // Should accept valid data with V3 wire format
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", PERSON_BEAN)))
                    .succeedsWithin(Duration.ofSeconds(10));
            consumer.subscribe(Set.of(topic.name()));

            var records = consumer.poll(Duration.ofSeconds(10));
            assertSingleRecordInTopicHasProperty(records, topic, ConsumerRecord::value, OBJECT_MAPPER.valueToTree(PERSON_BEAN));
        }
    }

    @Test
    void v2WireFormatValidationWorks(KafkaCluster cluster, Topic topic) {
        // Configure V2 wire format (8-byte global IDs) - deprecated but should still work
        var config = createContentIdRecordValidationConfigWithWireFormat(cluster, topic, "valueRule", firstContentId, "V2");

        var keySerde = new Serdes.StringSerde();
        // Create a V2-compatible producer (8-byte IDs)
        var producerValueSerde = createJsonSchemaProducerSerdeV2(false);
        var consumerValueSerde = createJsonSchemaConsumerSerdeV2(false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, producerValueSerde, Map.of());
                var consumer = consumeFromEarliestOffsets(tester, keySerde, consumerValueSerde)) {
            // Should accept valid data with V2 wire format
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", PERSON_BEAN)))
                    .succeedsWithin(Duration.ofSeconds(10));
            consumer.subscribe(Set.of(topic.name()));

            var records = consumer.poll(Duration.ofSeconds(10));
            assertSingleRecordInTopicHasProperty(records, topic, ConsumerRecord::value, OBJECT_MAPPER.valueToTree(PERSON_BEAN));
        }
    }

    @Test
    void v3ValidatorRejectsV2WireFormatData(KafkaCluster cluster, Topic topic) {
        // Configure V3 wire format validator
        var config = createContentIdRecordValidationConfigWithWireFormat(cluster, topic, "valueRule", firstContentId, "V3");

        var keySerde = new Serdes.StringSerde();
        // Produce data with V2 wire format (8-byte IDs)
        var producerValueSerde = createJsonSchemaProducerSerdeV2(false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, producerValueSerde, Map.of())) {
            // V3 validator should reject V2 format data
            var future = producer.send(new ProducerRecord<>(topic.name(), "my-key", PERSON_BEAN));
            // The validator should fail the produce request - wire format mismatch
            assertThatFutureFails(future, InvalidRecordException.class, "");
        }
    }

    @Test
    void defaultWireFormatIsV3(KafkaCluster cluster, Topic topic) {
        // When wireFormatVersion is not specified, it should default to V3
        var config = createContentIdRecordValidationConfig(cluster, topic, "valueRule", firstContentId);

        var keySerde = new Serdes.StringSerde();
        var producerValueSerde = createJsonSchemaProducerSerde(false, false);
        var consumerValueSerde = createJsonSchemaConsumerSerde(false, false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, producerValueSerde, Map.of());
                var consumer = consumeFromEarliestOffsets(tester, keySerde, consumerValueSerde)) {
            // Should accept V3 format by default
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", PERSON_BEAN)))
                    .succeedsWithin(Duration.ofSeconds(10));
            consumer.subscribe(Set.of(topic.name()));

            var records = consumer.poll(Duration.ofSeconds(10));
            assertSingleRecordInTopicHasProperty(records, topic, ConsumerRecord::value, OBJECT_MAPPER.valueToTree(PERSON_BEAN));
        }
    }

    private static ConfigurationBuilder createContentIdRecordValidationConfig(KafkaCluster cluster, Topic topic, String ruleType, int contentId) {
        String className = RecordValidation.class.getName();
        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(className, className).withConfig("rules",
                List.of(Map.of("topicNames", List.of(topic.name()), ruleType,
                        Map.of("schemaValidationConfig", Map.of("apicurioRegistryUrl", apicurioRegistryUrl, "apicurioId", contentId)))))
                .build();
        return proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());
    }

    private static ConfigurationBuilder createContentIdRecordValidationConfigWithWireFormat(KafkaCluster cluster, Topic topic, String ruleType, int contentId,
                                                                                            String wireFormatVersion) {
        String className = RecordValidation.class.getName();
        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(className, className).withConfig("rules",
                List.of(Map.of("topicNames", List.of(topic.name()), ruleType,
                        Map.of("schemaValidationConfig",
                                Map.of("apicurioRegistryUrl", apicurioRegistryUrl, "apicurioId", contentId, "wireFormatVersion", wireFormatVersion)))))
                .build();
        return proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());
    }

    private static @NonNull JsonSchemaSerde<PersonBean> createJsonSchemaProducerSerdeV2(boolean schemaIdInHeader) {
        // V2 wire format uses Legacy8ByteIdHandler
        var producerSerde = new JsonSchemaSerde<PersonBean>();
        producerSerde.configure(Map.of(
                SerdeConfig.REGISTRY_URL, apicurioRegistryUrl,
                SerdeConfig.EXPLICIT_ARTIFACT_ID, firstArtifactId,
                SerdeConfig.EXPLICIT_ARTIFACT_VERSION, "1",
                KafkaSerdeConfig.ENABLE_HEADERS, schemaIdInHeader,
                SerdeConfig.ID_HANDLER, "io.apicurio.registry.serde.Legacy8ByteIdHandler"), false);
        return producerSerde;
    }

    private static @NonNull JsonSchemaSerde<Object> createJsonSchemaConsumerSerdeV2(boolean schemaIdInHeader) {
        // V2 wire format uses Legacy8ByteIdHandler
        var consumerSerde = new JsonSchemaSerde<>();
        consumerSerde.configure(Map.of(
                SerdeConfig.EXPLICIT_ARTIFACT_ID, firstArtifactId,
                SerdeConfig.EXPLICIT_ARTIFACT_VERSION, "1",
                KafkaSerdeConfig.ENABLE_HEADERS, schemaIdInHeader,
                SerdeConfig.REGISTRY_URL, apicurioRegistryUrl,
                SerdeConfig.ID_HANDLER, "io.apicurio.registry.serde.Legacy8ByteIdHandler"), false);
        return consumerSerde;
    }

    private static <T, S> org.apache.kafka.clients.consumer.Consumer<T, S> consumeFromEarliestOffsets(KroxyliciousTester tester, Serde<T> keySerde,
                                                                                                      Serde<S> valueSerde) {
        return tester.consumer(keySerde, valueSerde, Map.of(
                GROUP_ID_CONFIG, "my-group-id",
                AUTO_OFFSET_RESET_CONFIG, "earliest"));
    }

    @AfterAll
    static void stopResources() {
        stopContainer(registryContainer);
    }
}
