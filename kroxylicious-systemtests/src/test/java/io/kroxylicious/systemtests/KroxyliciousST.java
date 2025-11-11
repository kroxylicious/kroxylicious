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
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;

import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.BDDAssumptions.given;

/**
 * The Kroxylicious system tests.
 */
class KroxyliciousST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousST.class);
    private static Kroxylicious kroxylicious;
    private final String clusterName = "my-cluster";
    protected static final String BROKER_NODE_NAME = "kafka";
    private static final String MESSAGE = "Hello-world";
    private KroxyliciousOperator kroxyliciousOperator;

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
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(clusterName);
        String bootstrap = kroxylicious.getBootstrap(clusterName);

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
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortIdentifiesNodeWithTlsAndNoFilters(clusterName);
        String bootstrap = kroxylicious.getBootstrap(clusterName);

        produceAndConsumeMessage(namespace, bootstrap);
    }

    @Test
    void moreThanOneServicesInDifferentNamespaces(String namespace) {
        LOGGER.atInfo().setMessage("Given Kroxylicious service in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(clusterName);

        String newNamespace = namespace + "-2";
        LOGGER.atInfo().setMessage("Given another Kroxylicious service in {} namespace with {} replicas").addArgument(newNamespace).addArgument(1).log();
        NamespaceUtils.createNamespaceAndPrepare(newNamespace);
        Kroxylicious kroxylicious2 = new Kroxylicious(newNamespace);
        kroxylicious2.deployPortIdentifiesNodeWithNoFilters(clusterName);

        String bootstrap = kroxylicious.getBootstrap(clusterName);
        assertThat(bootstrap).withFailMessage("bootstrap " + bootstrap + " does not contain the corresponding namespace " + namespace)
                .contains(namespace);
        produceAndConsumeMessage(namespace, bootstrap);

        bootstrap = kroxylicious2.getBootstrap(clusterName);
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

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1, compressionType);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

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
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(clusterName);
        String bootstrap = kroxylicious.getBootstrap(clusterName);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 1);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        LOGGER.atInfo().setMessage("And a kafka broker is restarted").log();
        KafkaSteps.restartKafkaBroker(clusterName);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(5));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

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

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(replicas).log();
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(clusterName, replicas);
        String bootstrap = kroxylicious.getBootstrap(clusterName);
        int currentReplicas = kroxylicious.getNumberOfReplicas();
        given(currentReplicas).withFailMessage("unexpected deployed replicas").isEqualTo(replicas);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 1);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
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

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(replicas).log();
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(clusterName, replicas);
        String bootstrap = kroxylicious.getBootstrap(clusterName);
        int currentReplicas = kroxylicious.getNumberOfReplicas();
        given(currentReplicas).withFailMessage("unexpected deployed replicas").isEqualTo(replicas);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 1);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("And kroxylicious is scaled to {}").addArgument(scaleTo).log();
        kroxylicious.scaleReplicasTo(scaleTo, Duration.ofMinutes(2));
        currentReplicas = kroxylicious.getNumberOfReplicas();
        assertThat(currentReplicas).withFailMessage("unexpected current scaled replicas").isEqualTo(scaleTo);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
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
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
        }
        else {
            LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

            int numberOfBrokers = 1;
            KafkaBuilder kafka = KafkaTemplates.kafkaPersistentWithKRaftAnnotations(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, numberOfBrokers);

            resourceManager.createResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(BROKER_NODE_NAME, kafka.build(), numberOfBrokers),
                    kafka);
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }
}
