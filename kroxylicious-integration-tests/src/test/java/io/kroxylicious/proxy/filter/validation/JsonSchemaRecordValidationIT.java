/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.validation;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.function.Consumer;

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
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.jsonschema.JsonSchemaSerde;
import io.apicurio.rest.client.util.IoUtil;

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

    private static final String JSON_MESSAGE = """
            {"firstName":"json1","lastName":"json2"}""";
    private static final String INVALID_AGE_MESSAGE = """
            {"firstName":"json1","lastName":"json2","age":-3}""";
    private static final String APICURIO_REGISTRY_HOST = "http://localhost";
    private static final Integer APICURIO_REGISTRY_PORT = 8081;
    private static final String APICURIO_REGISTRY_URL = APICURIO_REGISTRY_HOST + ":" + APICURIO_REGISTRY_PORT;
    private static final String FIRST_ARTIFACT_ID = UUID.randomUUID().toString();
    private static final String SECOND_ARTIFACT_ID = UUID.randomUUID().toString();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final PersonBean PERSON_BEAN = new PersonBean("john", "smith", 23);
    public static long firstGlobalId;
    public static long secondGlobalId;

    private static GenericContainer registryContainer;

    @BeforeAll
    public static void init() throws IOException {
        // An Apicurio Registry instance is required for this test to work, so we start one using a Generic Container
        DockerImageName dockerImageName = DockerImageName.parse("quay.io/apicurio/apicurio-registry-mem:2.6.13.Final");

        Consumer<CreateContainerCmd> cmd = e -> e.withPortBindings(
                new PortBinding(Ports.Binding.bindPort(APICURIO_REGISTRY_PORT), new ExposedPort(APICURIO_REGISTRY_PORT)));

        registryContainer = new GenericContainer<>(dockerImageName)
                .withEnv(Map.of(
                        "QUARKUS_HTTP_PORT", String.valueOf(APICURIO_REGISTRY_PORT),
                        "REGISTRY_APIS_V2_DATE_FORMAT", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
                .withExposedPorts(APICURIO_REGISTRY_PORT)
                .withCreateContainerCmdModifier(cmd);

        registryContainer.start();
        registryContainer.waitingFor(Wait.forLogMessage(".*Installed features:*", 1));

        // Preparation: In this test class, a schema already registered in Apicurio Registry with globalId one is expected, so we register it upfront.
        try (var client = RegistryClientFactory.create(APICURIO_REGISTRY_URL)) {
            firstGlobalId = client.createArtifact(null, FIRST_ARTIFACT_ID, IoUtil.toStream(JSON_SCHEMA_TOPIC_1)).getGlobalId();
            secondGlobalId = client.createArtifact(null, SECOND_ARTIFACT_ID, IoUtil.toStream(JSON_SCHEMA_TOPIC_1)).getGlobalId();
        }
    }

    @Test
    void shouldAcceptValidJsonInProduceRequest(KafkaCluster cluster, Topic topic) throws Exception {
        var config = createGlobalIdRecordValidationConfig(cluster, topic, "valueRule", firstGlobalId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            producer.send(new ProducerRecord<>(topic.name(), "my-key", JSON_MESSAGE)).get();

            var records = consumeAll(tester, topic);

            assertSingleRecordInTopicHasProperty(records, topic, ConsumerRecord::value, JSON_MESSAGE);
        }
    }

    @Test
    void invalidAgeProduceRejectedUsingTopicNames(KafkaCluster cluster, Topic topic1, Topic topic2) throws Exception {
        // Topic 2 has schema validation, invalid data cannot be sent.
        var config = createGlobalIdRecordValidationConfig(cluster, topic2, "valueRule", firstGlobalId);

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
        var config = createGlobalIdRecordValidationConfig(cluster, topic, "valueRule", 3L);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(topic.name(), "my-key", JSON_MESSAGE));
            assertThatFutureFails(invalid, InvalidRecordException.class, "No artifact with ID '3' in group 'null' was found");
        }
    }

    record PersonBean(String firstName, String lastName, int age) {}

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void clientSideUsesValueSchemasToo(boolean schemaIdInHeader, KafkaCluster cluster, Topic topic) throws Exception {
        var config = createGlobalIdRecordValidationConfig(cluster, topic, "valueRule", firstGlobalId);

        var keySerde = new Serdes.StringSerde();
        var producerValueSerde = createJsonSchemaProducerSerde(schemaIdInHeader, false);
        var consumerValueSerde = createJsonSchemaConsumerSerde(false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, producerValueSerde, Map.of());
                var consumer = consumeFromEarliestOffsets(tester, keySerde, consumerValueSerde)) {
            producer.send(new ProducerRecord<>(topic.name(), "my-key", PERSON_BEAN)).get();
            consumer.subscribe(Set.of(topic.name()));

            var records = consumer.poll(Duration.ofSeconds(10));

            // note that when the schemaid is in the value, there's no type information so the deserializer will give a JsonNode.
            var expected = schemaIdInHeader ? PERSON_BEAN : OBJECT_MAPPER.valueToTree(PERSON_BEAN);
            assertSingleRecordInTopicHasProperty(records, topic, ConsumerRecord::value, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void clientSideUsesKeySchemasToo(boolean schemaIdInHeader, KafkaCluster cluster, Topic topic) throws Exception {
        var config = createGlobalIdRecordValidationConfig(cluster, topic, "keyRule", firstGlobalId);

        boolean isKey = true;
        var producerKeySerde = createJsonSchemaProducerSerde(schemaIdInHeader, isKey);
        var consumerKeySerde = createJsonSchemaConsumerSerde(isKey);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerKeySerde, new Serdes.StringSerde(), Map.of());
                var consumer = consumeFromEarliestOffsets(tester, consumerKeySerde, new Serdes.StringSerde())) {
            producer.send(new ProducerRecord<>(topic.name(), PERSON_BEAN, "my-value")).get();
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
        var config = createGlobalIdRecordValidationConfig(cluster, topic, "valueRule", secondGlobalId);

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
        var config = createGlobalIdRecordValidationConfig(cluster, topic, "keyRule", secondGlobalId);

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

    private static @NonNull JsonSchemaSerde<Object> createJsonSchemaConsumerSerde(boolean isKey) {
        var consumerKeySerde = new JsonSchemaSerde<>();
        consumerKeySerde.configure(Map.of(
                SerdeConfig.REGISTRY_URL, APICURIO_REGISTRY_URL), isKey);
        return consumerKeySerde;
    }

    private static @NonNull JsonSchemaSerde<PersonBean> createJsonSchemaProducerSerde(boolean schemaIdInHeader, boolean isKey) {
        var producerKeySerde = new JsonSchemaSerde<PersonBean>();
        producerKeySerde.configure(Map.of(
                SerdeConfig.REGISTRY_URL, APICURIO_REGISTRY_URL,
                SerdeConfig.EXPLICIT_ARTIFACT_ID, FIRST_ARTIFACT_ID,
                SerdeConfig.ENABLE_HEADERS, schemaIdInHeader), isKey);
        return producerKeySerde;
    }

    private static ConfigurationBuilder createGlobalIdRecordValidationConfig(KafkaCluster cluster, Topic topic, String ruleType, long globalId) {
        String className = RecordValidation.class.getName();
        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(className, className).withConfig("rules",
                List.of(Map.of("topicNames", List.of(topic.name()), ruleType,
                        Map.of("schemaValidationConfig", Map.of("apicurioRegistryUrl", APICURIO_REGISTRY_URL, "apicurioGlobalId", globalId)))))
                .build();
        return proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());
    }

    private static <T, S> org.apache.kafka.clients.consumer.Consumer<T, S> consumeFromEarliestOffsets(KroxyliciousTester tester, Serde<T> keySerde,
                                                                                                      Serde<S> valueSerde) {
        return tester.consumer(keySerde, valueSerde, Map.of(
                GROUP_ID_CONFIG, "my-group-id",
                AUTO_OFFSET_RESET_CONFIG, "earliest"));
    }

    @AfterAll
    public static void stopResources() {
        if (registryContainer != null && registryContainer.isRunning()) {
            registryContainer.stop();
        }
    }

    static boolean isDockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }
}
