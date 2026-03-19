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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import io.apicurio.registry.client.RegistryClientFactory;
import io.apicurio.registry.client.common.RegistryClientOptions;
import io.apicurio.registry.rest.client.models.CreateArtifact;
import io.apicurio.registry.rest.client.models.CreateVersion;
import io.apicurio.registry.rest.client.models.VersionContent;
import io.apicurio.registry.rest.client.models.VersionMetaData;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.apicurio.registry.serde.protobuf.ProtobufSerde;

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
class ProtobufRecordValidationIT extends RecordValidationBaseIT {

    private static final String PROTOBUF_SCHEMA = """
            syntax = "proto3";

            message Person {
              string first_name = 1;
              string last_name = 2;
              int32 age = 3;
            }
            """;

    // Valid protobuf encoding of Person{first_name="John", last_name="Doe", age=25}
    // Field 1 (string): tag=0x0A, len=4, "John"
    // Field 2 (string): tag=0x12, len=3, "Doe"
    // Field 3 (int32): tag=0x18, varint=25
    private static final byte[] VALID_PROTOBUF = new byte[]{
            0x0A, 0x04, 'J', 'o', 'h', 'n',
            0x12, 0x03, 'D', 'o', 'e',
            0x18, 25
    };

    // Corrupt protobuf: length-delimited field claims length 16 but only 1 byte follows
    private static final byte[] CORRUPT_PROTOBUF = new byte[]{ 0x0A, 0x10, 0x01 };

    private static final String APICURIO_REGISTRY_HOST = "http://localhost";
    private static final Integer APICURIO_REGISTRY_PORT = 8083;
    private static final String APICURIO_REGISTRY_API = "/apis/registry/v3";
    private static final String APICURIO_REGISTRY_URL = APICURIO_REGISTRY_HOST + ":" + APICURIO_REGISTRY_PORT + APICURIO_REGISTRY_API;

    private static String artifactId;
    private static int contentId;
    private static GenericContainer<?> registryContainer;
    private static Descriptors.Descriptor personDescriptor;

    @BeforeAll
    static void init() throws Descriptors.DescriptorValidationException {
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
        createArtifact.setArtifactType("PROTOBUF");
        CreateVersion version = new CreateVersion();
        VersionContent content = new VersionContent();
        content.setContent(PROTOBUF_SCHEMA);
        content.setContentType("application/x-protobuf");
        version.setContent(content);
        createArtifact.setFirstVersion(version);

        VersionMetaData artifact = client.groups().byGroupId("default").artifacts().post(createArtifact).getVersion();
        artifactId = artifact.getArtifactId();
        contentId = artifact.getContentId().intValue();

        // Build descriptor for creating DynamicMessage instances in tests
        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setSyntax("proto3")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Person")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("first_name").setNumber(1).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("last_name").setNumber(2).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("age").setNumber(3).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)))
                .build();
        Descriptors.FileDescriptor fileDesc = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[]{});
        personDescriptor = fileDesc.findMessageTypeByName("Person");
    }

    @Test
    void shouldAcceptValidProtobufInProduceRequest(KafkaCluster cluster, Topic topic) {
        var config = createProtobufValidationConfig(cluster, topic, contentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(new Serdes.StringSerde(), Serdes.serdeFrom(new ByteArraySerializer(), new ByteArrayDeserializer()), Map.of())) {
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", VALID_PROTOBUF)))
                    .succeedsWithin(Duration.ofSeconds(10));

            try (var consumer = tester.consumer(new Serdes.StringSerde(), Serdes.serdeFrom(new ByteArraySerializer(), new ByteArrayDeserializer()),
                    Map.of(GROUP_ID_CONFIG, UUID.randomUUID().toString(), AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
                consumer.subscribe(Set.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(10));
                assertThat(records.records(topic.name()))
                        .hasSize(1)
                        .extracting(ConsumerRecord::value)
                        .first()
                        .isEqualTo(VALID_PROTOBUF);
            }
        }
    }

    @Test
    void shouldRejectCorruptProtobufInProduceRequest(KafkaCluster cluster, Topic topic) {
        var config = createProtobufValidationConfig(cluster, topic, contentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(new Serdes.StringSerde(), Serdes.serdeFrom(new ByteArraySerializer(), new ByteArrayDeserializer()), Map.of())) {
            var future = producer.send(new ProducerRecord<>(topic.name(), "my-key", CORRUPT_PROTOBUF));
            assertThatFutureFails(future, InvalidRecordException.class, "Failed to parse Protobuf message");
        }
    }

    @Test
    void shouldAcceptEmptyProtobufMessage(KafkaCluster cluster, Topic topic) {
        // In proto3, all fields are optional, so an empty message is valid
        var config = createProtobufValidationConfig(cluster, topic, contentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(new Serdes.StringSerde(), Serdes.serdeFrom(new ByteArraySerializer(), new ByteArrayDeserializer()), Map.of())) {
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", new byte[0])))
                    .succeedsWithin(Duration.ofSeconds(10));
        }
    }

    @Test
    void shouldAllowAnyDataOnUnvalidatedTopic(KafkaCluster cluster, Topic validatedTopic, Topic unvalidatedTopic) {
        var config = createProtobufValidationConfig(cluster, validatedTopic, contentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(new Serdes.StringSerde(), Serdes.serdeFrom(new ByteArraySerializer(), new ByteArrayDeserializer()), Map.of())) {
            // Corrupt data on unvalidated topic should succeed
            assertThat(producer.send(new ProducerRecord<>(unvalidatedTopic.name(), "my-key", CORRUPT_PROTOBUF)))
                    .succeedsWithin(Duration.ofSeconds(10));

            // Corrupt data on validated topic should fail
            var future = producer.send(new ProducerRecord<>(validatedTopic.name(), "my-key", CORRUPT_PROTOBUF));
            assertThatFutureFails(future, InvalidRecordException.class, "Failed to parse Protobuf message");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void clientSideProtobufSerdePassesValidation(boolean schemaIdInHeader, KafkaCluster cluster, Topic topic) {
        var config = createProtobufValidationConfig(cluster, topic, contentId);

        var keySerde = new Serdes.StringSerde();
        var producerValueSerde = createProtobufProducerSerde(schemaIdInHeader);
        var consumerValueSerde = createProtobufConsumerSerde(schemaIdInHeader);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, producerValueSerde, Map.of());
                var consumer = consumeFromEarliestOffsets(tester, keySerde, consumerValueSerde)) {
            DynamicMessage person = DynamicMessage.newBuilder(personDescriptor)
                    .setField(personDescriptor.findFieldByName("first_name"), "John")
                    .setField(personDescriptor.findFieldByName("last_name"), "Doe")
                    .setField(personDescriptor.findFieldByName("age"), 25)
                    .build();

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", person)))
                    .succeedsWithin(Duration.ofSeconds(10));
            consumer.subscribe(Set.of(topic.name()));

            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.records(topic.name()))
                    .hasSize(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void detectsClientProducingWithWrongProtobufSchemaId(boolean schemaIdInHeader, KafkaCluster cluster, Topic topic) {
        // Configure filter to expect a different contentId than the client sends
        var config = createProtobufValidationConfig(cluster, topic, contentId + 1);

        var keySerde = new Serdes.StringSerde();
        var valueSerde = createProtobufProducerSerde(schemaIdInHeader);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(keySerde, valueSerde, Map.of())) {
            DynamicMessage person = DynamicMessage.newBuilder(personDescriptor)
                    .setField(personDescriptor.findFieldByName("first_name"), "John")
                    .setField(personDescriptor.findFieldByName("last_name"), "Doe")
                    .setField(personDescriptor.findFieldByName("age"), 25)
                    .build();

            var future = producer.send(new ProducerRecord<>(topic.name(), "my-key", person));
            assertThatFutureFails(future, InvalidRecordException.class, "Unexpected schema id in record");
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static ProtobufSerde<DynamicMessage> createProtobufProducerSerde(boolean schemaIdInHeader) {
        var serde = new ProtobufSerde();
        serde.configure(Map.of(
                SerdeConfig.REGISTRY_URL, APICURIO_REGISTRY_URL,
                SerdeConfig.EXPLICIT_ARTIFACT_ID, artifactId,
                SerdeConfig.EXPLICIT_ARTIFACT_VERSION, "1",
                KafkaSerdeConfig.ENABLE_HEADERS, schemaIdInHeader), false);
        return serde;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static ProtobufSerde<DynamicMessage> createProtobufConsumerSerde(boolean schemaIdInHeader) {
        var serde = new ProtobufSerde();
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

    private static ConfigurationBuilder createProtobufValidationConfig(KafkaCluster cluster, Topic topic, int protobufContentId) {
        String className = RecordValidation.class.getName();
        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(className, className).withConfig("rules",
                List.of(Map.of("topicNames", List.of(topic.name()), "valueRule",
                        Map.of("schemaValidationConfig",
                                Map.of("apicurioRegistryUrl", APICURIO_REGISTRY_URL, "apicurioId", protobufContentId, "schemaType", "PROTOBUF")))))
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
