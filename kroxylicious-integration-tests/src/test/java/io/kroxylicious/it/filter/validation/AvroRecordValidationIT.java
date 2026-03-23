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
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

import io.apicurio.registry.client.RegistryClientFactory;
import io.apicurio.registry.client.common.RegistryClientOptions;
import io.apicurio.registry.rest.client.models.CreateArtifact;
import io.apicurio.registry.rest.client.models.CreateVersion;
import io.apicurio.registry.rest.client.models.VersionContent;
import io.apicurio.registry.rest.client.models.VersionMetaData;
import io.apicurio.registry.serde.avro.AvroSerde;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;

import io.kroxylicious.filter.validation.RecordValidation;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@EnabledIf(value = "isDockerAvailable", disabledReason = "docker unavailable")
class AvroRecordValidationIT extends RecordValidationBaseIT {

    private static final String AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "Person",
              "namespace": "io.kroxylicious.test",
              "fields": [
                {"name": "firstName", "type": "string"},
                {"name": "lastName", "type": "string"},
                {"name": "age", "type": "int"}
              ]
            }
            """;

    private static final String AVRO_SCHEMA_2 = """
            {
              "type": "record",
              "name": "Person2",
              "namespace": "io.kroxylicious.test",
              "fields": [
                {"name": "firstName", "type": "string"},
                {"name": "lastName", "type": "string"}
              ]
            }
            """;

    private static final String APICURIO_REGISTRY_HOST = "http://localhost";
    private static final Integer APICURIO_REGISTRY_PORT = 8082;
    private static final String APICURIO_REGISTRY_API = "/apis/registry/v3";
    private static final String APICURIO_REGISTRY_URL = APICURIO_REGISTRY_HOST + ":" + APICURIO_REGISTRY_PORT + APICURIO_REGISTRY_API;

    private static String artifactId;
    private static int contentId;
    private static int secondContentId;
    private static GenericContainer<?> registryContainer;
    private static Schema parsedSchema;

    @BeforeAll
    static void init() {
        String image = "quay.io/apicurio/apicurio-registry:3.1.6@sha256:d0625211cebb1f58a2982df29cb0945249d8f88f37ccce9e162c0c12c2aea89e";
        DockerImageName dockerImageName = DockerImageName.parse(image)
                .asCompatibleSubstituteFor(DockerImageName.parse(image.substring(0, image.indexOf("@"))));

        Consumer<CreateContainerCmd> cmd = e -> e.withHostConfig(new HostConfig().withPortBindings(
                new PortBinding(Ports.Binding.bindPort(APICURIO_REGISTRY_PORT), new ExposedPort(APICURIO_REGISTRY_PORT))));

        registryContainer = new GenericContainer<>(dockerImageName)
                .withEnv(Map.of("QUARKUS_HTTP_PORT", String.valueOf(APICURIO_REGISTRY_PORT)))
                .withExposedPorts(APICURIO_REGISTRY_PORT)
                .withCreateContainerCmdModifier(cmd)
                .waitingFor(Wait.forHttp(APICURIO_REGISTRY_API + "/system/info").forStatusCode(200));

        registryContainer.start();

        var client = RegistryClientFactory.create(RegistryClientOptions.create(APICURIO_REGISTRY_URL));

        CreateArtifact createArtifact = new CreateArtifact();
        createArtifact.setArtifactType("AVRO");
        CreateVersion version = new CreateVersion();
        VersionContent content = new VersionContent();
        content.setContent(AVRO_SCHEMA);
        content.setContentType("application/json");
        version.setContent(content);
        createArtifact.setFirstVersion(version);

        VersionMetaData artifact = client.groups().byGroupId("default").artifacts().post(createArtifact).getVersion();
        artifactId = artifact.getArtifactId();
        contentId = artifact.getContentId().intValue();

        // Register a second schema to get a different contentId for wrong-schema-id tests
        CreateArtifact createSecondArtifact = new CreateArtifact();
        createSecondArtifact.setArtifactType("AVRO");
        CreateVersion secondVersion = new CreateVersion();
        VersionContent secondContent = new VersionContent();
        secondContent.setContent(AVRO_SCHEMA_2);
        secondContent.setContentType("application/json");
        secondVersion.setContent(secondContent);
        createSecondArtifact.setFirstVersion(secondVersion);

        VersionMetaData secondArtifact = client.groups().byGroupId("default").artifacts().post(createSecondArtifact).getVersion();
        secondContentId = secondArtifact.getContentId().intValue();

        parsedSchema = new Schema.Parser().parse(AVRO_SCHEMA);
    }

    @Test
    void shouldAcceptValidAvroInProduceRequest(KafkaCluster cluster, Topic topic) {
        var config = createAvroValidationConfig(cluster, topic, contentId);

        var keySerde = new Serdes.StringSerde();
        var producerValueSerde = createAvroProducerSerde(false);
        var consumerValueSerde = createAvroConsumerSerde(false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, producerValueSerde, Map.of());
                var consumer = consumeFromEarliestOffsets(tester, keySerde, consumerValueSerde)) {
            GenericRecord person = new GenericData.Record(parsedSchema);
            person.put("firstName", "John");
            person.put("lastName", "Doe");
            person.put("age", 25);

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", person)))
                    .succeedsWithin(Duration.ofSeconds(10));

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.records(topic.name()))
                    .hasSize(1)
                    .extracting(ConsumerRecord::value)
                    .first()
                    .satisfies(value -> {
                        GenericRecord received = (GenericRecord) value;
                        assertThat(received.get("firstName").toString()).isEqualTo("John");
                        assertThat(received.get("lastName").toString()).isEqualTo("Doe");
                        assertThat(received.get("age")).isEqualTo(25);
                    });
        }
    }

    @Test
    void shouldRejectInvalidAvroInProduceRequest(KafkaCluster cluster, Topic topic) {
        var config = createAvroValidationConfig(cluster, topic, contentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var future = producer.send(new ProducerRecord<>(topic.name(), "my-key", "not avro data"));
            assertThatFutureFails(future, InvalidRecordException.class, "Failed to deserialize Avro record");
        }
    }

    @Test
    void shouldAllowValidAvroOnUnvalidatedTopic(KafkaCluster cluster, Topic validatedTopic, Topic unvalidatedTopic) {
        var config = createAvroValidationConfig(cluster, validatedTopic, contentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            // Invalid data on unvalidated topic should succeed
            assertThat(producer.send(new ProducerRecord<>(unvalidatedTopic.name(), "my-key", "not avro data")))
                    .succeedsWithin(Duration.ofSeconds(10));

            // Invalid data on validated topic should fail
            var future = producer.send(new ProducerRecord<>(validatedTopic.name(), "my-key", "not avro data"));
            assertThatFutureFails(future, InvalidRecordException.class, "Failed to deserialize Avro record");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void clientSideAvroSerdePassesValidation(boolean schemaIdInHeader, KafkaCluster cluster, Topic topic) {
        var config = createAvroValidationConfig(cluster, topic, contentId);

        var keySerde = new Serdes.StringSerde();
        var producerValueSerde = createAvroProducerSerde(schemaIdInHeader);
        var consumerValueSerde = createAvroConsumerSerde(schemaIdInHeader);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, producerValueSerde, Map.of());
                var consumer = consumeFromEarliestOffsets(tester, keySerde, consumerValueSerde)) {
            GenericRecord person = new GenericData.Record(parsedSchema);
            person.put("firstName", "John");
            person.put("lastName", "Doe");
            person.put("age", 30);

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", person)))
                    .succeedsWithin(Duration.ofSeconds(10));
            consumer.subscribe(Set.of(topic.name()));

            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.records(topic.name()))
                    .hasSize(1)
                    .extracting(ConsumerRecord::value)
                    .first()
                    .satisfies(value -> {
                        GenericRecord received = (GenericRecord) value;
                        assertThat(received.get("firstName").toString()).isEqualTo("John");
                        assertThat(received.get("lastName").toString()).isEqualTo("Doe");
                        assertThat(received.get("age")).isEqualTo(30);
                    });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void detectsClientProducingWithWrongAvroSchemaId(boolean schemaIdInHeader, KafkaCluster cluster, Topic topic) {
        // Configure filter to expect a different contentId than the client sends
        var config = createAvroValidationConfig(cluster, topic, secondContentId);

        var keySerde = new Serdes.StringSerde();
        var valueSerde = createAvroProducerSerde(schemaIdInHeader);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, valueSerde, Map.of())) {
            GenericRecord person = new GenericData.Record(parsedSchema);
            person.put("firstName", "John");
            person.put("lastName", "Doe");
            person.put("age", 30);

            var future = producer.send(new ProducerRecord<>(topic.name(), "my-key", person));
            assertThatFutureFails(future, InvalidRecordException.class, "Unexpected schema id in record");
        }
    }

    private static AvroSerde<GenericRecord> createAvroProducerSerde(boolean schemaIdInHeader) {
        var serde = new AvroSerde<GenericRecord>();
        serde.configure(Map.of(
                SerdeConfig.REGISTRY_URL, APICURIO_REGISTRY_URL,
                SerdeConfig.EXPLICIT_ARTIFACT_ID, artifactId,
                SerdeConfig.EXPLICIT_ARTIFACT_VERSION, "1",
                KafkaSerdeConfig.ENABLE_HEADERS, schemaIdInHeader), false);
        return serde;
    }

    private static AvroSerde<GenericRecord> createAvroConsumerSerde(boolean schemaIdInHeader) {
        var serde = new AvroSerde<GenericRecord>();
        serde.configure(Map.of(
                SerdeConfig.REGISTRY_URL, APICURIO_REGISTRY_URL,
                SerdeConfig.EXPLICIT_ARTIFACT_ID, artifactId,
                SerdeConfig.EXPLICIT_ARTIFACT_VERSION, "1",
                KafkaSerdeConfig.ENABLE_HEADERS, schemaIdInHeader), false);
        return serde;
    }

    private static <T, S> org.apache.kafka.clients.consumer.Consumer<T, S> consumeFromEarliestOffsets(KroxyliciousTester tester, Serde<T> keySerde,
                                                                                                      Serde<S> valueSerde) {
        return tester.consumer(keySerde, valueSerde, Map.of(
                GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                AUTO_OFFSET_RESET_CONFIG, "earliest"));
    }

    private static ConfigurationBuilder createAvroValidationConfig(KafkaCluster cluster, Topic topic, int avroContentId) {
        String className = RecordValidation.class.getName();
        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(className, className).withConfig("rules",
                List.of(Map.of("topicNames", List.of(topic.name()), "valueRule",
                        Map.of("schemaValidationConfig",
                                Map.of("apicurioRegistryUrl", APICURIO_REGISTRY_URL, "apicurioId", avroContentId, "schemaType", "AVRO")))))
                .build();
        return proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());
    }

    @AfterAll
    static void stopResources() {
        if (registryContainer != null && registryContainer.isRunning()) {
            registryContainer.stop();
        }
    }

    static boolean isDockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }
}
