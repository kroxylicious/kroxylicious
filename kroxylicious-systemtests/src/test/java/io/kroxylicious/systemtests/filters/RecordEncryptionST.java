/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.filters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import org.apache.kafka.common.record.CompressionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;

import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.TestKmsFacadeFactory;
import io.kroxylicious.systemtests.AbstractSystemTests;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousBuilder;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.kms.ExperimentalKmsConfig;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousFilterTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaClusterRefTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyIngressTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousVirtualKafkaClusterTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@ParameterizedClass(autoCloseArguments = true)
@MethodSource("facadesSource")
class RecordEncryptionST extends AbstractSystemTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordEncryptionST.class);
    private static final String MESSAGE = "Hello-world";
    private static final String KEK_PREFIX = "KEK-";
    private final String clusterName = "record-encryption-cluster";
    private String bootstrap;
    private TestKekManager testKekManager;
    private KroxyliciousOperator kroxyliciousOperator;
    private Kroxylicious kroxylicious;

    static Stream<? extends TestKmsFacade<?, ?, ?>> facadesSource() {
        // We rely on the fact that streams are lazy so the facade isn't built
        // or started until the first test needs it.
        return TestKmsFacadeFactory.getTestKmsFacadeFactories()
                .filter(f -> f.getClass().getName().contains("systemtests"))
                .map(TestKmsFacadeFactory::build)
                .filter(TestKmsFacade::isAvailable)
                .peek(TestKmsFacade::start);
    }

    @Parameter
    TestKmsFacade<?, ?, ?> testKmsFacade;

    @BeforeAll
    void setUp() {
        KafkaClients.getKafkaClient().preloadImage();
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
        }
        else {
            LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

            int kafkaReplicas = 1;
            resourceManager.createResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.poolWithDualRoleAndPersistentStorage(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas),
                    KafkaTemplates.defaultKafka(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas));
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    @BeforeEach
    void beforeEach() {
        bootstrap = null;
        testKekManager = null;
    }

    @AfterAll
    void cleanUp() {
        kroxyliciousOperator.delete();
    }

    @AfterEach
    void afterEach(String namespace) {
        try {
            if (testKekManager != null) {
                LOGGER.atInfo().log("Deleting KEK...");
                testKekManager.deleteKek(KEK_PREFIX + topicName);
            }
        }
        catch (KubeClusterException e) {
            LOGGER.atError().setMessage("KEK deletion has not been successfully done: {}").addArgument(e).log();
            throw e;
        }
        finally {
            if (bootstrap != null) {
                KafkaSteps.deleteTopic(namespace, topicName, bootstrap);
            }
        }
    }

    private void deployPortIdentifiesNodeWithRecordEncryptionFilter(TestKmsFacade<?, ?, ?> testKmsFacade) {
        deployPortIdentifiesNodeWithRecordEncryptionFilter(testKmsFacade, null);
    }

    private void deployPortIdentifiesNodeWithRecordEncryptionFilter(TestKmsFacade<?, ?, ?> testKmsFacade, ExperimentalKmsConfig experimentalKmsConfig) {
        String filterName = Constants.KROXYLICIOUS_ENCRYPTION_FILTER_NAME + "-" + testKmsFacade.getKmsServiceClass().getSimpleName().toLowerCase(Locale.ROOT);
        kroxylicious = new KroxyliciousBuilder()
                .withNamespace(Constants.KROXYLICIOUS_NAMESPACE)
                .withKafkaProxy(KroxyliciousKafkaProxyTemplates.defaultKafkaProxyCR(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, 1).build())
                .withKafkaProxyIngress(KroxyliciousKafkaProxyIngressTemplates
                        .defaultKafkaProxyIngressCR(Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME).build())
                .withKafkaService(KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefCR(clusterName).build())
                .addKafkaProtocolFilter(
                        KroxyliciousFilterTemplates.kroxyliciousRecordEncryptionFilter(Constants.KROXYLICIOUS_NAMESPACE,
                                filterName, testKmsFacade, experimentalKmsConfig).build())
                .withVirtualKafkaCluster(KroxyliciousVirtualKafkaClusterTemplates.virtualKafkaClusterWithFilterCR(clusterName,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP,
                        List.of(filterName)).build())
                .build();
        kroxylicious.createOrUpdateResources();
    }

    @Test
    void ensureClusterHasEncryptedMessage(String namespace) {
        testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(KEK_PREFIX + topicName);
        int numberOfMessages = 1;

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, 1);
        deployPortIdentifiesNodeWithRecordEncryptionFilter(testKmsFacade);
        bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        LOGGER.info("And a kafka Topic named {}", topicName);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        ExecResult produceResult = KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        assertThat(produceResult.isSuccess()).withFailMessage("Unable to produce messages! " + produceResult.err()).isTrue();

        LOGGER.info("Then the messages are consumed");
        List<ConsumerRecord> resultEncrypted = KroxyliciousSteps.consumeMessageFromKafkaCluster(namespace, topicName, clusterName,
                Constants.KAFKA_DEFAULT_NAMESPACE, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: {}", resultEncrypted);

        assertAll(
                () -> assertThat(resultEncrypted.stream())
                        .withFailMessage("expected header has not been received!")
                        .allMatch(r -> r.getRecordHeaders().containsKey("kroxylicious.io/encryption")),
                () -> assertThat(resultEncrypted.stream())
                        .withFailMessage("Encrypted message still includes the original one!")
                        .allMatch(r -> !r.getPayload().contains(MESSAGE)));
    }

    /**
     * Produce and consume compressed message.
     *
     * @param compressionType the compression type
     * @param namespace the namespace
     */
    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void produceAndConsumeMessage(CompressionType compressionType, String namespace) {
        testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(KEK_PREFIX + topicName);
        int numberOfMessages = 1;

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, 1);
        // Deploy kroxylicious operand only the first compression iteration (1 per testKmsFacade)
        if (compressionType.ordinal() == 0) {
            deployPortIdentifiesNodeWithRecordEncryptionFilter(testKmsFacade);
        }
        bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        LOGGER.info("And a kafka Topic named {}", topicName);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1, compressionType);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        ExecResult produceResult = KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, compressionType, numberOfMessages);
        assertThat(produceResult.isSuccess()).withFailMessage("Unable to produce records! " + produceResult.err()).isTrue();

        LOGGER.info("Then the messages are consumed");
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: {}", result);

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    @SuppressWarnings("java:S2925")
    @Test
    void ensureClusterHasEncryptedMessageWithRotatedKEK(String namespace) {
        // Skip AWS test execution because the ciphertext blob metadata to read the version of the KEK is not available anywhere
        assumeThat(isVaultKms(testKmsFacade)).isTrue();
        String kekAlias = KEK_PREFIX + topicName;
        testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(kekAlias);
        int numberOfMessages = 1;
        ExperimentalKmsConfig experimentalKmsConfig = new ExperimentalKmsConfig(null, null, null, 5L);

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, 1);
        deployPortIdentifiesNodeWithRecordEncryptionFilter(testKmsFacade, experimentalKmsConfig);
        bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        LOGGER.info("And a kafka Topic named {}", topicName);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        ExecResult produceResult = KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        assertThat(produceResult.isSuccess()).withFailMessage("Unable to produce messages! " + produceResult.err()).isTrue();

        LOGGER.info("Then the messages are consumed");
        List<ConsumerRecord> resultEncrypted = KroxyliciousSteps.consumeMessageFromKafkaCluster(namespace, topicName, clusterName,
                Constants.KAFKA_DEFAULT_NAMESPACE, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: {}", resultEncrypted);

        assertKekVersionWithinParcel(resultEncrypted, ":v1:", testKekManager);

        LOGGER.info("When KEK is rotated");
        testKekManager.rotateKek(kekAlias);

        try {
            Thread.sleep(5000);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        LOGGER.info("And {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.info("Then the messages are consumed");
        List<ConsumerRecord> resultEncryptedRotatedKek = KroxyliciousSteps.consumeMessageFromKafkaCluster(namespace, topicName, clusterName,
                Constants.KAFKA_DEFAULT_NAMESPACE, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: {}", resultEncryptedRotatedKek);

        List<ConsumerRecord> finalEncryptedResults = new ArrayList<>(resultEncryptedRotatedKek);
        finalEncryptedResults.removeAll(resultEncrypted);
        assertKekVersionWithinParcel(finalEncryptedResults, ":v2:", testKekManager);
    }

    private void assertKekVersionWithinParcel(List<ConsumerRecord> consumerRecords, String expectedValue, TestKekManager testKekManager) {
        assertThat(consumerRecords)
                .withFailMessage("expected messages not received! Consumer records is empty")
                .isNotEmpty();
        assertThat(testKekManager.getClass().getSimpleName().toLowerCase())
                .withFailMessage("Another KMS different from Vault is not currently supported!")
                .startsWith("vault");

        assertThat(consumerRecords.stream())
                .withFailMessage(expectedValue + " is not contained in the ciphertext blob!")
                .allMatch(r -> r.getPayload().contains(expectedValue));
    }

    @SuppressWarnings("java:S2925")
    @Test
    void produceAndConsumeMessageWithRotatedKEK(String namespace) {
        String kekAlias = KEK_PREFIX + topicName;
        testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(kekAlias);
        int numberOfMessages = 1;
        boolean isVaultKms = isVaultKms(testKmsFacade);
        Long resolvedAliasExpireAfterWriteSeconds = isVaultKms ? null : 5L;
        Long resolvedDekExpireAfterWriteSeconds = isVaultKms ? 5L : null;
        ExperimentalKmsConfig experimentalKmsConfig = new ExperimentalKmsConfig(resolvedAliasExpireAfterWriteSeconds, null, null, resolvedDekExpireAfterWriteSeconds);

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, 1);
        deployPortIdentifiesNodeWithRecordEncryptionFilter(testKmsFacade, experimentalKmsConfig);
        bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        LOGGER.info("And a kafka Topic named {}", topicName);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        ExecResult produceResult = KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        assertThat(produceResult.isSuccess()).withFailMessage("Unable to produce messages! " + produceResult.err()).isTrue();

        LOGGER.info("Then the messages are consumed");
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: {}", result);

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));

        LOGGER.info("When KEK is rotated");
        testKekManager.rotateKek(kekAlias);

        try {
            Thread.sleep(5000);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        LOGGER.info("And {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        produceResult = KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        assertThat(produceResult.isSuccess()).withFailMessage("Unable to produce messages! " + produceResult.err()).isTrue();

        LOGGER.info("Then the messages are consumed");
        List<ConsumerRecord> resultRotatedKek = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: {}", resultRotatedKek);

        assertThat(resultRotatedKek).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    private boolean isVaultKms(TestKmsFacade<?, ?, ?> testKmsFacade) {
        LOGGER.debug("Checking if Vault Kms is used");
        return testKmsFacade.getKmsServiceClass().getSimpleName().toLowerCase().startsWith("vault");
    }
}
