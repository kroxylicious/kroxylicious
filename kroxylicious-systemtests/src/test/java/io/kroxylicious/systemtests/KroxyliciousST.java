/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.common.record.CompressionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;

import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousBuilder;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;
import io.kroxylicious.systemtests.utils.KroxyliciousUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.BDDAssumptions.given;

/**
 * The Kroxylicious system tests.
 */
class KroxyliciousST extends AbstractSystemTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousST.class);
    private static Kroxylicious kroxylicious;
    private final String clusterName = "kroxylicious-st-cluster";
    private static final String MESSAGE = "Hello-world";
    private KroxyliciousOperator kroxyliciousOperator;

    private void deployPortIdentifiesNodeWithNoFilters(int replicas) {
        kroxylicious = KroxyliciousBuilder.singleNodeBaseBuilder(Constants.KROXYLICIOUS_NAMESPACE, clusterName, replicas).build();
        kroxylicious.createOrUpdateResources();
    }

    private void deployPortIdentifiesNodeWithNoFilters() {
        deployPortIdentifiesNodeWithNoFilters(1);
    }

    /**
     * Produce and consume message.
     *
     * @param namespace the namespace
     * @param compressionType the compression type
     */
    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void produceAndConsumeCompressedMessages(CompressionType compressionType, String namespace) {
        // start Kroxylicious
        deployPortIdentifiesNodeWithNoFilters();
        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        produceAndConsumeMessage(namespace, bootstrap, compressionType);
    }

    /**
     * Produce and consume message with TLS.
     *
     * @param namespace the namespace
     */
    @Test
    void produceAndConsumeMessagesWithTls(String namespace) {
        // start Kroxylicious
        Tls tls = KroxyliciousUtils.createCertificateConfigMapFromListener(Constants.KROXYLICIOUS_NAMESPACE);
        kroxylicious = KroxyliciousBuilder.singleNodeBaseBuilder(Constants.KROXYLICIOUS_NAMESPACE, clusterName, 1)
                .withTls(tls)
                .build();
        kroxylicious.createOrUpdateResources();
        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        produceAndConsumeMessage(namespace, bootstrap);
    }

    @Test
    void moreThanOneServicesInDifferentNamespaces(String namespace) {
        deployPortIdentifiesNodeWithNoFilters();

        String newNamespace = namespace + "-2";
        NamespaceUtils.createNamespaceAndPrepare(newNamespace);

        Kroxylicious kroxylicious2 = KroxyliciousBuilder.singleNodeBaseBuilder(newNamespace, clusterName, 1).build();
        kroxylicious2.createOrUpdateResources();

        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);
        assertThat(bootstrap).withFailMessage("bootstrap " + bootstrap + " does not contain the corresponding namespace " + Constants.KROXYLICIOUS_NAMESPACE)
                .contains(Constants.KROXYLICIOUS_NAMESPACE);
        produceAndConsumeMessage(namespace, bootstrap);

        bootstrap = kroxylicious2.getBootstrap(newNamespace, clusterName);
        assertThat(bootstrap).withFailMessage("bootstrap " + bootstrap + " does not contain the corresponding namespace " + newNamespace)
                .contains(newNamespace);
        produceAndConsumeMessage(newNamespace, bootstrap, randomTopicName(), CompressionType.NONE);
    }

    private void produceAndConsumeMessage(String namespace, String bootstrap) {
        produceAndConsumeMessage(namespace, bootstrap, topicName, CompressionType.NONE);
    }

    private void produceAndConsumeMessage(String namespace, String bootstrap, CompressionType compressionType) {
        produceAndConsumeMessage(namespace, bootstrap, topicName, compressionType);
    }

    private void produceAndConsumeMessage(String namespace, String bootstrap, String topicName, CompressionType compressionType) {
        int numberOfMessages = 1;

        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1, compressionType);

        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, compressionType, numberOfMessages);

        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    /**
     * Restart kafka brokers.
     *
     * @param namespace the namespace
     */
    @Test
    void restartKafkaBrokers(String namespace) {
        int numberOfMessages = 25;

        // start Kroxylicious
        deployPortIdentifiesNodeWithNoFilters();
        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 1);

        KroxyliciousSteps.produceMessagesWithoutWait(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        KafkaSteps.restartKafkaBroker(clusterName);

        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(5));

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    /**
     * Kroxylicious with replicas.
     *
     * @param namespace the namespace
     */
    @Test
    void kroxyWithReplicas(String namespace) {
        int numberOfMessages = 3;
        int replicas = 3;

        // Clean up the kroxylicious instance to assure number of replicas
        cleanUpKroxyliciousInstance();

        // start Kroxylicious
        deployPortIdentifiesNodeWithNoFilters(replicas);
        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);
        int currentReplicas = kroxylicious.getNumberOfReplicas(Constants.KROXYLICIOUS_NAMESPACE);
        given(currentReplicas).withFailMessage("unexpected deployed replicas: " + currentReplicas).isEqualTo(replicas);

        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 1);

        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));

        // Scale down to 1 replica to restore kroxylicious
        kroxylicious.scaleReplicasTo(Constants.KROXYLICIOUS_NAMESPACE, 1, Duration.ofMinutes(2));
    }

    @Test
    void scaleUpKroxylicious(String namespace) {
        scaleKroxylicious(namespace, 2, 3);
    }

    @Test
    void scaleDownKroxylicious(String namespace) {
        scaleKroxylicious(namespace, 3, 2);
    }

    private void scaleKroxylicious(String namespace, int replicas, int scaleTo) {
        int numberOfMessages = 10;

        // Clean up the kroxylicious instance to assure number of replicas
        cleanUpKroxyliciousInstance();

        // start Kroxylicious
        deployPortIdentifiesNodeWithNoFilters(replicas);
        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);
        int currentReplicas = kroxylicious.getNumberOfReplicas(Constants.KROXYLICIOUS_NAMESPACE);
        given(currentReplicas).withFailMessage("unexpected deployed replicas: " + currentReplicas).isEqualTo(replicas);

        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 1);

        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        kroxylicious.scaleReplicasTo(Constants.KROXYLICIOUS_NAMESPACE, scaleTo, Duration.ofMinutes(2));
        currentReplicas = kroxylicious.getNumberOfReplicas(Constants.KROXYLICIOUS_NAMESPACE);
        assertThat(currentReplicas).withFailMessage("unexpected current scaled replicas").isEqualTo(scaleTo);

        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));

        // Scale down to 1 replica to restore kroxylicious
        kroxylicious.scaleReplicasTo(Constants.KROXYLICIOUS_NAMESPACE, 1, Duration.ofMinutes(2));
    }

    @AfterAll
    void cleanUp() {
        if (kroxyliciousOperator != null) {
            kroxyliciousOperator.delete();
        }
    }

    /**
     * Sets before all.
     */
    @BeforeAll
    void setupBefore() {
        KafkaClients.getKafkaClient().preloadImage();
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
        }
        else {
            LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

            int kafkaReplicas = 1;
            resourceManager.createOrUpdateResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.poolWithDualRoleAndPersistentStorage(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas),
                    KafkaTemplates.defaultKafka(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas));
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    private void cleanUpKroxyliciousInstance() {
        NamespaceUtils.deleteNamespaceWithWait(Constants.KROXYLICIOUS_NAMESPACE);
        NamespaceUtils.createNamespaceAndPrepare(Constants.KROXYLICIOUS_NAMESPACE);
    }
}
