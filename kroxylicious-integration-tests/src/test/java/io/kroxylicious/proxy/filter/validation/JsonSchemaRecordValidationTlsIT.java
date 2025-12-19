/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.validation;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.apicurio.registry.client.RegistryClientFactory;
import io.apicurio.registry.client.RegistryClientOptions;
import io.apicurio.registry.rest.client.models.CreateArtifact;
import io.apicurio.registry.rest.client.models.CreateVersion;
import io.apicurio.registry.rest.client.models.VersionContent;
import io.apicurio.registry.rest.client.models.VersionMetaData;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.tls.CertificateGenerator;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for schema validation with TLS-protected Apicurio Registry.
 * Tests that Kroxylicious can connect to a schema registry using a custom trust store.
 */
@ExtendWith(KafkaClusterExtension.class)
@EnabledIf(value = "isDockerAvailable", disabledReason = "docker unavailable")
class JsonSchemaRecordValidationTlsIT extends RecordValidationBaseIT {

    private static final String JSON_SCHEMA = """
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
                }
              }
            }
            """;

    private static final String VALID_JSON_MESSAGE = """
            {"firstName":"John","lastName":"Doe"}""";

    private static final Integer APICURIO_REGISTRY_HTTPS_PORT = 8443;
    private static final String APICURIO_REGISTRY_API = "/apis/registry/v3";

    private static GenericContainer<?> registryContainer;
    private static String apicurioRegistryUrl;
    private static int contentId;
    private static String trustStorePath;
    private static String trustStorePassword;
    private static String trustStoreType;

    @BeforeAll
    static void init() throws Exception {
        // Generate certificates and keystores using CertificateGenerator
        CertificateGenerator.Keys keys = CertificateGenerator.generate();

        // Use the generated JKS keystore for the registry server
        CertificateGenerator.KeyStore jksKeystore = keys.jksServerKeystore();

        // Use the generated PKCS12 truststore for the client (Kroxylicious)
        CertificateGenerator.TrustStore pkcs12Truststore = keys.pkcs12ClientTruststore();
        trustStorePath = pkcs12Truststore.path().toString();
        trustStorePassword = pkcs12Truststore.password();
        trustStoreType = pkcs12Truststore.type();

        // Start Apicurio Registry with TLS enabled
        DockerImageName dockerImageName = DockerImageName.parse("quay.io/apicurio/apicurio-registry:3.0.7");

        registryContainer = new GenericContainer<>(dockerImageName)
                .withEnv(Map.of(
                        "QUARKUS_HTTP_SSL_PORT", String.valueOf(APICURIO_REGISTRY_HTTPS_PORT),
                        "QUARKUS_HTTP_SSL_CERTIFICATE_KEY_STORE_FILE", "/opt/keystore.jks",
                        "QUARKUS_HTTP_SSL_CERTIFICATE_KEY_STORE_PASSWORD", jksKeystore.storePassword(),
                        "QUARKUS_HTTP_SSL_CERTIFICATE_KEY_STORE_FILE_TYPE", CertificateGenerator.JKS,
                        "QUARKUS_HTTP_SSL_CERTIFICATE_KEY_STORE_KEY_PASSWORD", jksKeystore.keyPassword(),
                        "QUARKUS_HTTP_INSECURE_REQUESTS", "disabled"))
                .withExposedPorts(APICURIO_REGISTRY_HTTPS_PORT)
                .withCopyFileToContainer(MountableFile.forHostPath(jksKeystore.path().toString()), "/opt/keystore.jks")
                .waitingFor(Wait.forHttps(APICURIO_REGISTRY_API + "/system/info")
                        .forStatusCode(200)
                        .allowInsecure());

        registryContainer.start();

        Integer mappedPort = registryContainer.getMappedPort(APICURIO_REGISTRY_HTTPS_PORT);
        apicurioRegistryUrl = "https://localhost:" + mappedPort + APICURIO_REGISTRY_API;

        // Register schema using HTTPS (with insecure client for setup)
        var clientOptions = RegistryClientOptions.create(apicurioRegistryUrl).trustAll(true);
        var client = RegistryClientFactory.create(clientOptions);

        CreateArtifact createArtifact = new CreateArtifact();
        createArtifact.setArtifactType("JSON");
        CreateVersion version = new CreateVersion();
        VersionContent content = new VersionContent();
        content.setContent(JSON_SCHEMA);
        content.setContentType("application/json");
        version.setContent(content);
        createArtifact.setFirstVersion(version);

        VersionMetaData artifact = client.groups().byGroupId("default").artifacts().post(createArtifact).getVersion();
        contentId = artifact.getContentId().intValue();
    }

    @Test
    void shouldValidateRecordsWhenRegistryIsTlsProtectedWithTrustStore(KafkaCluster cluster, Topic topic) throws Exception {
        // Configure schema validation with TLS trust store
        var config = createTlsSchemaValidationConfig(cluster, topic, contentId, trustStorePath, trustStorePassword, trustStoreType);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            // Should successfully validate and produce
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic.name(), "my-key", VALID_JSON_MESSAGE)).get();

            var records = consumeAll(tester, topic);
            assertSingleRecordInTopicHasValue(records, topic, VALID_JSON_MESSAGE);
        }
    }

    @Test
    void shouldFailWhenTlsConfigurationIsMissing(KafkaCluster cluster, Topic topic) {
        // Configure schema validation WITHOUT TLS trust store - should fail to connect
        var config = createSchemaValidationConfigWithoutTls(cluster, topic, contentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            // Should fail because the registry uses TLS with a self-signed cert and no trust store is configured
            var future = producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic.name(), "my-key", VALID_JSON_MESSAGE));
            assertThatFutureFails(future, org.apache.kafka.common.InvalidRecordException.class, "");
        }
    }

    @Test
    void shouldSucceedWithInsecureTlsConfiguration(KafkaCluster cluster, Topic topic) throws Exception {
        // Configure schema validation with insecure TLS (trust all)
        var config = createInsecureTlsSchemaValidationConfig(cluster, topic, contentId);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            // Should successfully validate and produce because insecure=true trusts all certificates
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic.name(), "my-key", VALID_JSON_MESSAGE)).get();

            var records = consumeAll(tester, topic);
            assertSingleRecordInTopicHasValue(records, topic, VALID_JSON_MESSAGE);
        }
    }

    private static ConfigurationBuilder createTlsSchemaValidationConfig(KafkaCluster cluster, Topic topic, int contentId,
                                                                        String trustStorePath, String trustStorePassword, String trustStoreType) {
        String className = RecordValidation.class.getName();
        Map<String, Object> tlsConfig = Map.of(
                "trust", Map.of(
                        "storeFile", trustStorePath,
                        "storePassword", Map.of("password", trustStorePassword),
                        "storeType", trustStoreType));

        Map<String, Object> schemaValidationConfig = Map.of(
                "apicurioRegistryUrl", apicurioRegistryUrl,
                "apicurioContentId", contentId,
                "tls", tlsConfig);

        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(className, className)
                .withConfig("rules", List.of(Map.of(
                        "topicNames", List.of(topic.name()),
                        "valueRule", Map.of("schemaValidationConfig", schemaValidationConfig))))
                .build();

        return proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());
    }

    private static ConfigurationBuilder createSchemaValidationConfigWithoutTls(KafkaCluster cluster, Topic topic, int contentId) {
        String className = RecordValidation.class.getName();
        Map<String, Object> schemaValidationConfig = Map.of(
                "apicurioRegistryUrl", apicurioRegistryUrl,
                "apicurioContentId", contentId);

        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(className, className)
                .withConfig("rules", List.of(Map.of(
                        "topicNames", List.of(topic.name()),
                        "valueRule", Map.of("schemaValidationConfig", schemaValidationConfig))))
                .build();

        return proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());
    }

    private static ConfigurationBuilder createInsecureTlsSchemaValidationConfig(KafkaCluster cluster, Topic topic, int contentId) {
        String className = RecordValidation.class.getName();
        Map<String, Object> tlsConfig = Map.of(
                "trust", Map.of("insecure", true));

        Map<String, Object> schemaValidationConfig = Map.of(
                "apicurioRegistryUrl", apicurioRegistryUrl,
                "apicurioContentId", contentId,
                "tls", tlsConfig);

        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(className, className)
                .withConfig("rules", List.of(Map.of(
                        "topicNames", List.of(topic.name()),
                        "valueRule", Map.of("schemaValidationConfig", schemaValidationConfig))))
                .build();

        return proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());
    }

    private static void assertSingleRecordInTopicHasValue(ConsumerRecords<String, String> records, Topic topic, String expectedValue) {
        assertThat(records.records(topic.name()))
                .hasSize(1)
                .map(ConsumerRecord::value)
                .containsExactly(expectedValue);
    }

    @Override
    public ConsumerRecords<String, String> consumeAll(KroxyliciousTester tester, Topic topic) {
        try (var consumer = tester.consumer(Map.of(GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis(), AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            consumer.subscribe(Set.of(topic.name()));
            return consumer.poll(Duration.ofSeconds(10));
        }
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
