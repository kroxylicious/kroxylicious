/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.Kafka;

import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;

import static io.kroxylicious.systemtests.TestTags.EXTERNAL_KAFKA_CLIENTS;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The non-JVM clients system tests.
 */
@Tag(EXTERNAL_KAFKA_CLIENTS)
class NonJVMClientsST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(NonJVMClientsST.class);
    private final String clusterName = "my-cluster";
    private final String clusterIpServiceName = clusterName + "-cluster-ip";
    protected static final String BROKER_NODE_NAME = "kafka";
    private static final String MESSAGE = "Hello-world";
    private String bootstrap;
    private KroxyliciousOperator kroxyliciousOperator;

    /**
     * Produce and consume message with kcat.
     *
     * @param namespace the namespace
     */
    @Test
    void produceAndConsumeWithKcatClients(String namespace) {
        int numberOfMessages = 2;
        LOGGER.atInfo().setMessage("When the message '{}' is sent to the topic '{}'").addArgument(MESSAGE).addArgument(topicName).log();
        KafkaClients.kcat().inNamespace(namespace).produceMessages(topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KafkaClients.kcat().inNamespace(namespace).consumeMessages(topicName, bootstrap, numberOfMessages,
                Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getValue)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    /**
     * Produce and consume message with kaf.
     *
     * @param namespace the namespace
     */
    @Test
    void produceAndConsumeWithKafkaGoClients(String namespace) {
        int numberOfMessages = 2;
        LOGGER.atInfo().setMessage("When the message '{}' is sent to the topic '{}'").addArgument(MESSAGE).addArgument(topicName).log();
        KafkaClients.kaf().inNamespace(namespace).produceMessages(topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KafkaClients.kaf().inNamespace(namespace).consumeMessages(topicName, bootstrap, numberOfMessages,
                Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getValue)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    /**
     * Produce with kcat and consume message with java test clients.
     *
     * @param namespace the namespace
     */
    @Test
    void produceWithKcatAndConsumeWithTestClients(String namespace) {
        int numberOfMessages = 2;
        LOGGER.atInfo().setMessage("When the message '{}' is sent to the topic '{}'").addArgument(MESSAGE).addArgument(topicName).log();
        KafkaClients.kcat().inNamespace(namespace).produceMessages(topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KafkaClients.strimziTestClient().inNamespace(namespace).consumeMessages(topicName, bootstrap, numberOfMessages,
                Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getValue)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    /**
     * Produce with java test clients and consume message with kcat.
     *
     * @param namespace the namespace
     */
    @Test
    void produceWithTestClientsAndConsumeWithKcat(String namespace) {
        int numberOfMessages = 2;
        LOGGER.atInfo().setMessage("When the message '{}' is sent to the topic '{}'").addArgument(MESSAGE).addArgument(topicName).log();
        KafkaClients.strimziTestClient().inNamespace(namespace).produceMessages(topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KafkaClients.kcat().inNamespace(namespace).consumeMessages(topicName, bootstrap, numberOfMessages,
                Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getValue)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    /**
     * Produce with kaf and consume message with java test clients.
     *
     * @param namespace the namespace
     */
    @Test
    void produceWithKafkaGoAndConsumeWithTestClients(String namespace) {
        int numberOfMessages = 2;
        LOGGER.atInfo().setMessage("When the message '{}' is sent to the topic '{}'").addArgument(MESSAGE).addArgument(topicName).log();
        KafkaClients.kaf().inNamespace(namespace).produceMessages(topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KafkaClients.strimziTestClient().inNamespace(namespace).consumeMessages(topicName, bootstrap, numberOfMessages,
                Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getValue)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    /**
     * Produce with java test clients and consume message with kaf.
     *
     * @param namespace the namespace
     */
    @Test
    void produceWithTestClientsAndConsumeWithKafkaGo(String namespace) {
        int numberOfMessages = 2;
        LOGGER.atInfo().setMessage("When the message '{}' is sent to the topic '{}'").addArgument(MESSAGE).addArgument(topicName).log();
        KafkaClients.strimziTestClient().inNamespace(namespace).produceMessages(topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KafkaClients.kaf().inNamespace(namespace).consumeMessages(topicName, bootstrap, numberOfMessages,
                Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getValue)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    @BeforeEach
    void setUpBeforeEach(String namespace) {
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();

        Kroxylicious kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(clusterName);
        bootstrap = kroxylicious.getBootstrap(clusterIpServiceName);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);
    }

    /**
     * Sets before all.
     */
    @BeforeAll
    void setupBefore() {
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.warn("Skipping kafka deployment. It is already deployed!");
            return;
        }
        LOGGER.info("Deploying Kafka in {} namespace", Constants.KAFKA_DEFAULT_NAMESPACE);

        Kafka kafka = KafkaTemplates.kafkaPersistentWithKRaftAnnotations(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, 3).build();

        resourceManager.createResourceWithWait(
                KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(BROKER_NODE_NAME, kafka, 3).build(),
                kafka);

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    @AfterAll
    void cleanUp() {
        kroxyliciousOperator.delete();
    }
}
