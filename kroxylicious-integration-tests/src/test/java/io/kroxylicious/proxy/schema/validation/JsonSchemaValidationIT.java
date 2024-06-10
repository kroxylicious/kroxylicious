/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.schema.validation;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.rest.client.util.IoUtil;

import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.proxy.filter.schema.ProduceValidationFilterFactory;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
class JsonSchemaValidationIT extends SchemaValidationBaseIT {

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

    private static final String JSON_MESSAGE = "{\"firstName\":\"json1\",\"lastName\":\"json2\"}";
    private static final String INVALID_AGE_MESSAGE = "{\"firstName\":\"json1\",\"lastName\":\"json2\",\"age\":-3}";
    private static final String TOPIC_1 = "my-test-topic";
    private static final String TOPIC_2 = "my-test-topic-2";

    private static final String APICURIO_REGISTRY_HOST = "http://localhost";
    private static final Integer APICURIO_REGISTRY_PORT = 8081;
    private static final String APICURIO_REGISTRY_URL = APICURIO_REGISTRY_HOST + ":" + APICURIO_REGISTRY_PORT;

    private static GenericContainer registryContainer;

    @BeforeAll
    public static void init() throws IOException {
        // An Apicurio Registry instance is required for this test to work, so we start one using a Generic Container
        DockerImageName dockerImageName = DockerImageName.parse("quay.io/apicurio/apicurio-registry-mem")
                .withTag("2.5.11.Final");

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
            client.createArtifact(null, UUID.randomUUID().toString(), IoUtil.toStream(JSON_SCHEMA_TOPIC_1));
        }
    }

    @Test
    void testValidJsonProduceAccepted(KafkaCluster cluster, Admin admin) throws Exception {
        assertThat(cluster.getNumOfBrokers()).isOne();
        createTopic(admin, TOPIC_1, 1);

        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName()).withConfig("rules",
                        List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                                Map.of("allowsNulls", true,
                                        "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true),
                                        "schemaValidationConfig", Map.of("apicurioRegistryUrl", APICURIO_REGISTRY_URL, "useApicurioGlobalId", 1L)))))
                        .build());

        try (var tester = kroxyliciousTester(config);
                var producer = getProducer(tester, 0, 16384);
                var consumer = getConsumer(tester)) {
            producer.send(new ProducerRecord<>(TOPIC_1, "my-key", JSON_MESSAGE)).get();
            consumer.subscribe(Set.of(TOPIC_1));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isOne();
            assertThat(records.iterator().next().value()).isEqualTo(JSON_MESSAGE);
        }
    }

    @Test
    void testInvalidAgeProduceRejectedUsingTopicNames(KafkaCluster cluster, Admin admin) throws Exception {
        assertThat(cluster.getNumOfBrokers()).isOne();
        createTopics(admin, new NewTopic(TOPIC_1, 1, (short) 1), new NewTopic(TOPIC_2, 1, (short) 1));

        // Topic 2 has schema validation, invalid data cannot be sent.
        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName()).withConfig("rules",
                        List.of(Map.of("topicNames", List.of(TOPIC_2), "valueRule",
                                Map.of("allowsNulls", true,
                                        "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true),
                                        "schemaValidationConfig", Map.of("apicurioRegistryUrl", APICURIO_REGISTRY_URL, "useApicurioGlobalId", 1L)))))
                        .build());

        try (var tester = kroxyliciousTester(config);
                var producer = getProducer(tester, 0, 16384);
                var consumer = getConsumer(tester)) {
            // Topic 2 has schema validation defined, invalid data cannot be produced.
            Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_2, "my-key", INVALID_AGE_MESSAGE));
            assertInvalidRecordExceptionThrown(invalid, "$.age: must have a minimum value of 0");

            // Topic 1 has no schema validation, invalid data is produced.
            producer.send(new ProducerRecord<>(TOPIC_1, "my-key", INVALID_AGE_MESSAGE)).get();
            consumer.subscribe(Set.of(TOPIC_1));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isOne();
            assertThat(records.iterator().next().value()).isEqualTo(INVALID_AGE_MESSAGE);
        }
    }

    @Test
    void testNonExistentSchema(KafkaCluster cluster, Admin admin) throws Exception {
        assertThat(cluster.getNumOfBrokers()).isOne();
        createTopic(admin, TOPIC_1, 1);

        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName()).withConfig("rules",
                        List.of(Map.of("topicNames", List.of(TOPIC_1), "valueRule",
                                Map.of("allowsNulls", true,
                                        "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true),
                                        "schemaValidationConfig", Map.of("apicurioRegistryUrl", APICURIO_REGISTRY_URL, "useApicurioGlobalId", 3L)))))
                        .build());

        try (var tester = kroxyliciousTester(config);
                var producer = getProducer(tester, 0, 16384)) {
            Future<RecordMetadata> invalid = producer.send(new ProducerRecord<>(TOPIC_1, "my-key", JSON_MESSAGE));
            assertInvalidRecordExceptionThrown(invalid, "No artifact with ID '3' in group 'null' was found");
        }
    }

    @AfterAll
    public static void stopResources() {
        if (registryContainer != null && registryContainer.isRunning()) {
            registryContainer.stop();
        }
    }
}
