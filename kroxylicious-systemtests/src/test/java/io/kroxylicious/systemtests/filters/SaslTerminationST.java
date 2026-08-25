/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.filters;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.scram.credentialstore.file.ScramCredentialFileManager;
import io.kroxylicious.systemtests.AbstractSystemTests;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.enums.KafkaClientType;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousBuilder;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousFilterTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousVirtualKafkaClusterTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

class SaslTerminationST extends AbstractSystemTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(SaslTerminationST.class);
    private static final String MESSAGE = "Hello-world";
    private static final String KEYSTORE_PASSWORD = "keystore-password-secret-456";
    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASSWORD = UUID.randomUUID().toString().replace("-", "");
    private static final String ALICE_USER = "alice";
    private static final String ALICE_PASSWORD = UUID.randomUUID().toString().replace("-", "");
    private static final String OAUTH_CLIENT_ID = "clientId-" + UUID.randomUUID();
    private static final String OAUTH_CLIENT_SECRET = "clientSecret";
    private static final String SCRAM_FILTER_NAME = "sasl-termination-scram";
    private static final String OAUTH_FILTER_NAME = "sasl-termination-oauth";
    private static final String MOCK_OAUTH_SERVER_NAME = "mock-oauth2-server";
    private static final int MOCK_OAUTH_SERVER_PORT = 8080;

    private final String clusterName = "sasl-termination-st-cluster";
    private KroxyliciousOperator kroxyliciousOperator;
    private static Kroxylicious kroxylicious;

    @BeforeAll
    void setUp() {
        KafkaClients.getKafkaClient().preloadImage();
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().log("Skipping kafka deployment. It is already deployed!");
        }
        else {
            LOGGER.atInfo()
                    .addKeyValue("namespace", Constants.KAFKA_DEFAULT_NAMESPACE)
                    .log("Deploying Kafka (no auth)");

            int kafkaReplicas = 1;
            resourceManager.createOrUpdateResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.poolWithDualRoleAndPersistentStorage(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas),
                    KafkaTemplates.defaultKafka(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas));
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    @BeforeEach
    void deleteExistingProxy() {
        var client = kubeClient().getClient();
        var proxy = client.resources(KafkaProxy.class)
                .inNamespace(Constants.KROXYLICIOUS_NAMESPACE)
                .withName(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME)
                .get();
        if (proxy != null) {
            LOGGER.atInfo().log("Deleting existing proxy to ensure clean state");
            client.resources(KafkaProxy.class)
                    .inNamespace(Constants.KROXYLICIOUS_NAMESPACE)
                    .withName(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME)
                    .delete();
            client.apps().deployments()
                    .inNamespace(Constants.KROXYLICIOUS_NAMESPACE)
                    .withName(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME)
                    .waitUntilCondition(d -> d == null, 60, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    @AfterAll
    void cleanUp() {
        if (kroxyliciousOperator != null) {
            kroxyliciousOperator.delete();
        }
    }

    @Test
    void testScramSha256Authentication(String namespace, @TempDir Path tempDir) throws Exception {
        // kcat does not support SCRAM authentication
        assumeThat(Environment.KAFKA_CLIENT).isNotEqualToIgnoringCase(KafkaClientType.KCAT.name());

        int numberOfMessages = 1;

        // Given
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, ADMIN_USER, ADMIN_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, ALICE_USER, ALICE_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        createKeystoreSecret(Constants.KROXYLICIOUS_NAMESPACE, "scram-keystore", "credentials.jks", keystorePath);
        createPasswordSecret(Constants.KROXYLICIOUS_NAMESPACE, "scram-keystore-password", KEYSTORE_PASSWORD);

        kroxylicious = KroxyliciousBuilder.singleNodeBaseBuilder(Constants.KROXYLICIOUS_NAMESPACE, clusterName, 1)
                .addKafkaProtocolFilter(KroxyliciousFilterTemplates.kroxyliciousSaslTerminationScramFilter(
                        Constants.KROXYLICIOUS_NAMESPACE, SCRAM_FILTER_NAME, "SCRAM-SHA-256",
                        "scram-keystore", "credentials.jks",
                        "scram-keystore-password", "password").build())
                .withVirtualKafkaCluster(KroxyliciousVirtualKafkaClusterTemplates.virtualKafkaClusterWithFilterCR(
                        clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP,
                        List.of(SCRAM_FILTER_NAME)).build())
                .build();
        kroxylicious.createOrUpdateResources();

        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        // When
        String kafkaBootstrap = clusterName + "-kafka-bootstrap." + Constants.KAFKA_DEFAULT_NAMESPACE + ".svc.cluster.local:9092";
        KafkaSteps.createTopic(namespace, topicName, kafkaBootstrap, 1, 1);

        Map<String, String> aliceProps = scramSaslProps(ALICE_USER, ALICE_PASSWORD);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages, aliceProps);

        // Then
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2), aliceProps);
        LOGGER.atInfo()
                .addKeyValue("received", result)
                .log("Consumed messages");

        assertThat(result)
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    @Disabled("Blocked by KAFKA-20184: Kafka 4.1+ eagerly loads jose4j via DefaultJwtValidator"
            + " during OAUTHBEARER client login, but the Strimzi test-clients image does not bundle jose4j")
    @Test
    void testOauthBearerAuthentication(String namespace) {
        // kcat does not support OAUTHBEARER authentication
        assumeThat(Environment.KAFKA_CLIENT).isNotEqualToIgnoringCase(KafkaClientType.KCAT.name());

        int numberOfMessages = 1;

        // Given
        deployMockOAuth2Server(Constants.KROXYLICIOUS_NAMESPACE);

        String baseUrl = "http://" + MOCK_OAUTH_SERVER_NAME + "." + Constants.KROXYLICIOUS_NAMESPACE + ".svc.cluster.local:" + MOCK_OAUTH_SERVER_PORT;
        String jwksUrl = baseUrl + "/default/jwks";
        String tokenUrl = baseUrl + "/default/token";
        String issuer = baseUrl + "/default";

        kroxylicious = KroxyliciousBuilder.singleNodeBaseBuilder(Constants.KROXYLICIOUS_NAMESPACE, clusterName, 1)
                .addKafkaProtocolFilter(KroxyliciousFilterTemplates.kroxyliciousSaslTerminationOauthFilter(
                        Constants.KROXYLICIOUS_NAMESPACE, OAUTH_FILTER_NAME, jwksUrl, "default", issuer).build())
                .withVirtualKafkaCluster(KroxyliciousVirtualKafkaClusterTemplates.virtualKafkaClusterWithFilterCR(
                        clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP,
                        List.of(OAUTH_FILTER_NAME)).build())
                .build();
        kroxylicious.createOrUpdateResources();

        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        Map<String, String> oauthProps = oauthSaslProps(tokenUrl);
        Map<String, String> allowedUrlsSystemProps = Map.of(
                "org.apache.kafka.sasl.oauthbearer.allowed.urls", tokenUrl + "," + jwksUrl);

        // When
        String kafkaBootstrap = clusterName + "-kafka-bootstrap." + Constants.KAFKA_DEFAULT_NAMESPACE + ".svc.cluster.local:9092";
        KafkaSteps.createTopic(namespace, topicName, kafkaBootstrap, 1, 1);

        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages, oauthProps, allowedUrlsSystemProps);

        // Then
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2), oauthProps,
                allowedUrlsSystemProps);
        LOGGER.atInfo()
                .addKeyValue("received", result)
                .log("Consumed messages");

        assertThat(result)
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    private Map<String, String> scramSaslProps(String username, String password) {
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
                .formatted(username, password);
        return new HashMap<>(Map.of(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256",
                SaslConfigs.SASL_JAAS_CONFIG, jaasConfig,
                "sasl.username", username,
                "sasl.password", password));
    }

    private Map<String, String> oauthSaslProps(String tokenEndpointUrl) {
        return new HashMap<>(Map.of(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, "OAUTHBEARER",
                SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;",
                SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler",
                "sasl.oauthbearer.token.endpoint.url", tokenEndpointUrl,
                "sasl.oauthbearer.client.credentials.client.id", OAUTH_CLIENT_ID,
                "sasl.oauthbearer.client.credentials.client.secret", OAUTH_CLIENT_SECRET));
    }

    private void createKeystoreSecret(String namespace, String secretName, String dataKey, Path keystorePath) {
        try {
            byte[] keystoreBytes = Files.readAllBytes(keystorePath);
            String encoded = Base64.getEncoder().encodeToString(keystoreBytes);
            var secret = new SecretBuilder()
                    .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(namespace)
                    .endMetadata()
                    .addToData(dataKey, encoded)
                    .build();
            kubeClient().getClient().secrets().inNamespace(namespace).resource(secret).create();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read keystore file: " + keystorePath, e);
        }
    }

    private void createPasswordSecret(String namespace, String secretName, String password) {
        var secret = new SecretBuilder()
                .withNewMetadata()
                .withName(secretName)
                .withNamespace(namespace)
                .endMetadata()
                .withStringData(Map.of("password", password))
                .build();
        kubeClient().getClient().secrets().inNamespace(namespace).resource(secret).create();
    }

    private void deployMockOAuth2Server(String namespace) {
        LOGGER.atInfo()
                .addKeyValue("namespace", namespace)
                .log("Deploying mock-oauth2-server");

        Map<String, String> labels = Map.of("app", MOCK_OAUTH_SERVER_NAME);

        var deployment = new DeploymentBuilder()
                .withNewMetadata()
                .withName(MOCK_OAUTH_SERVER_NAME)
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .withReplicas(1)
                .withNewSelector()
                .withMatchLabels(labels)
                .endSelector()
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withContainers(new ContainerBuilder()
                        .withName(MOCK_OAUTH_SERVER_NAME)
                        .withImage("ghcr.io/navikt/mock-oauth2-server:5.0.2")
                        .withPorts(new ContainerPortBuilder()
                                .withContainerPort(MOCK_OAUTH_SERVER_PORT)
                                .build())
                        .withEnv(
                                new EnvVarBuilder().withName("SERVER_PORT").withValue(String.valueOf(MOCK_OAUTH_SERVER_PORT)).build(),
                                new EnvVarBuilder().withName("LOG_LEVEL").withValue("DEBUG").build())
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        var service = new ServiceBuilder()
                .withNewMetadata()
                .withName(MOCK_OAUTH_SERVER_NAME)
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .withSelector(labels)
                .withPorts(new ServicePortBuilder()
                        .withPort(MOCK_OAUTH_SERVER_PORT)
                        .withTargetPort(new IntOrString(MOCK_OAUTH_SERVER_PORT))
                        .build())
                .endSpec()
                .build();

        kubeClient().getClient().apps().deployments().inNamespace(namespace).resource(deployment).create();
        kubeClient().getClient().services().inNamespace(namespace).resource(service).create();
        DeploymentUtils.waitForDeploymentReady(namespace, MOCK_OAUTH_SERVER_NAME);
    }
}
