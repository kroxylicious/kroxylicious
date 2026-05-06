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
import org.testcontainers.containers.GenericContainer;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

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
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
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

    private static final String PROTOBUF_SCHEMA_2 = """
            syntax = "proto3";

            message Address {
              string street = 1;
              string city = 2;
            }
            """;

    private static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";

    private static String apicurioRegistryUrl;
    private static String artifactId;
    private static int contentId;
    private static int secondContentId;
    private static GenericContainer<?> registryContainer;
    private static Descriptors.Descriptor personDescriptor;

    @BeforeAll
    static void init() throws Descriptors.DescriptorValidationException {
        registryContainer = startRegistryContainer();
        apicurioRegistryUrl = registryUrl(registryContainer);

        var client = registryClient(apicurioRegistryUrl);

        CreateArtifact createArtifact = new CreateArtifact();
        createArtifact.setArtifactType("PROTOBUF");
        CreateVersion version = new CreateVersion();
        VersionContent content = new VersionContent();
        content.setContent(PROTOBUF_SCHEMA);
        content.setContentType(PROTOBUF_CONTENT_TYPE);
        version.setContent(content);
        createArtifact.setFirstVersion(version);

        VersionMetaData artifact = client.groups().byGroupId("default").artifacts().post(createArtifact).getVersion();
        artifactId = artifact.getArtifactId();
        contentId = artifact.getContentId().intValue();

        // Register a second schema to get a different contentId for wrong-schema-id tests
        CreateArtifact createSecondArtifact = new CreateArtifact();
        createSecondArtifact.setArtifactType("PROTOBUF");
        CreateVersion secondVersion = new CreateVersion();
        VersionContent secondContent = new VersionContent();
        secondContent.setContent(PROTOBUF_SCHEMA_2);
        secondContent.setContentType(PROTOBUF_CONTENT_TYPE);
        secondVersion.setContent(secondContent);
        createSecondArtifact.setFirstVersion(secondVersion);

        VersionMetaData secondArtifact = client.groups().byGroupId("default").artifacts().post(createSecondArtifact).getVersion();
        secondContentId = secondArtifact.getContentId().intValue();

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

        var keySerde = new Serdes.StringSerde();
        var producerValueSerde = createProtobufProducerSerde(false);
        var consumerValueSerde = createProtobufConsumerSerde(false);

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
                    .hasSize(1)
                    .extracting(ConsumerRecord::value)
                    .first()
                    .satisfies(value -> {
                        DynamicMessage received = (DynamicMessage) value;
                        var desc = received.getDescriptorForType();
                        assertThat(received.getField(desc.findFieldByName("first_name"))).isEqualTo("John");
                        assertThat(received.getField(desc.findFieldByName("last_name"))).isEqualTo("Doe");
                        assertThat(received.getField(desc.findFieldByName("age"))).isEqualTo(25);
                    });
        }
    }

    @Test
    void shouldRejectCorruptProtobufInProduceRequest(KafkaCluster cluster, Topic topic) {
        var config = createProtobufValidationConfig(cluster, topic, contentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var future = producer.send(new ProducerRecord<>(topic.name(), "my-key", "corrupt protobuf data"));
            assertThatFutureFails(future, InvalidRecordException.class, "Failed to parse Protobuf message");
        }
    }

    @Test
    void shouldAllowAnyDataOnUnvalidatedTopic(KafkaCluster cluster, Topic validatedTopic, Topic unvalidatedTopic) {
        var config = createProtobufValidationConfig(cluster, validatedTopic, contentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            // Corrupt data on unvalidated topic should succeed
            assertThat(producer.send(new ProducerRecord<>(unvalidatedTopic.name(), "my-key", "corrupt protobuf data")))
                    .succeedsWithin(Duration.ofSeconds(10));

            // Corrupt data on validated topic should fail
            var future = producer.send(new ProducerRecord<>(validatedTopic.name(), "my-key", "corrupt protobuf data"));
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
                    .hasSize(1)
                    .extracting(ConsumerRecord::value)
                    .first()
                    .satisfies(value -> {
                        DynamicMessage received = (DynamicMessage) value;
                        var desc = received.getDescriptorForType();
                        assertThat(received.getField(desc.findFieldByName("first_name"))).isEqualTo("John");
                        assertThat(received.getField(desc.findFieldByName("last_name"))).isEqualTo("Doe");
                        assertThat(received.getField(desc.findFieldByName("age"))).isEqualTo(25);
                    });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void detectsClientProducingWithWrongProtobufSchemaId(boolean schemaIdInHeader, KafkaCluster cluster, Topic topic) {
        // Configure filter to expect a different contentId than the client sends
        var config = createProtobufValidationConfig(cluster, topic, secondContentId);

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
                SerdeConfig.REGISTRY_URL, apicurioRegistryUrl,
                SerdeConfig.EXPLICIT_ARTIFACT_ID, artifactId,
                SerdeConfig.EXPLICIT_ARTIFACT_VERSION, "1",
                KafkaSerdeConfig.ENABLE_HEADERS, schemaIdInHeader), false);
        return serde;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static ProtobufSerde<DynamicMessage> createProtobufConsumerSerde(boolean schemaIdInHeader) {
        // Override the message type header name to prevent the deserializer from using the
        // Java class name header (which causes reflection failures with DynamicMessage).
        // The deserializer falls back to Ref-based type detection in the body instead.
        var serde = new ProtobufSerde();
        serde.configure(Map.of(
                SerdeConfig.REGISTRY_URL, apicurioRegistryUrl,
                SerdeConfig.EXPLICIT_ARTIFACT_ID, artifactId,
                SerdeConfig.EXPLICIT_ARTIFACT_VERSION, "1",
                KafkaSerdeConfig.ENABLE_HEADERS, schemaIdInHeader,
                KafkaSerdeConfig.HEADER_VALUE_MESSAGE_TYPE_OVERRIDE_NAME, "x-ignore-msg-type"), false);
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
                                Map.of("apicurioRegistryUrl", apicurioRegistryUrl, "apicurioId", protobufContentId, "schemaType", "PROTOBUF")))))
                .build();
        return proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());
    }

    @AfterAll
    static void stopResources() {
        stopContainer(registryContainer);
    }
}
