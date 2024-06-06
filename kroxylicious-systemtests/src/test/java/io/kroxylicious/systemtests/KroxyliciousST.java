/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.Kafka;

import io.kroxylicious.systemtests.extensions.KroxyliciousExtension;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.BDDAssumptions.given;

/**
 * The Kroxylicious system tests.
 */
@ExtendWith(KroxyliciousExtension.class)
class KroxyliciousST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousST.class);
    private static Kroxylicious kroxylicious;
    private final String clusterName = "my-cluster";
    protected static final String BROKER_NODE_NAME = "kafka";
    private static final String MESSAGE = "Hello-world";

    /**
     * Produce and consume message.
     *
     * @param namespace the namespace
     */
    @Test
    void produceAndConsumeMessage(String namespace) {
        int numberOfMessages = 1;

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlainWithNoFilters(clusterName, 1);
        String bootstrap = kroxylicious.getBootstrap();

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord<String, String>> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        int numOfMessagesReceived = (int) result.stream().filter(c -> c.value().contains(MESSAGE)).count();
        assertThat(numOfMessagesReceived).withFailMessage("expected messages have not been received!").isEqualTo(numberOfMessages);
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
        kroxylicious.deployPortPerBrokerPlainWithNoFilters(clusterName, 1);
        String bootstrap = kroxylicious.getBootstrap();

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 2);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        LOGGER.atInfo().setMessage("And a kafka broker is restarted").log();
        KafkaSteps.restartKafkaBroker(clusterName);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord<String, String>> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(15));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();
        int numOfMessagesReceived = (int) result.stream().filter(c -> c.value().contains(MESSAGE)).count();
        assertThat(numOfMessagesReceived).withFailMessage("expected messages have not been received!").isEqualTo(numberOfMessages);
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
        kroxylicious.deployPortPerBrokerPlainWithNoFilters(clusterName, replicas);
        String bootstrap = kroxylicious.getBootstrap();
        int currentReplicas = kroxylicious.getNumberOfReplicas();
        given(currentReplicas).withFailMessage("unexpected deployed replicas").isEqualTo(replicas);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 2);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord<String, String>> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();
        int numOfMessagesReceived = (int) result.stream().filter(c -> c.value().contains(MESSAGE)).count();
        assertThat(numOfMessagesReceived).withFailMessage("expected messages have not been received!").isEqualTo(numberOfMessages);
    }

    @Test
    void scaleUpKroxylicious(String namespace) {
        int replicas = 2;
        scaleKroxylicious(namespace, replicas, replicas + 1);
    }

    @Test
    void scaleDownKroxylicious(String namespace) {
        int replicas = 3;
        scaleKroxylicious(namespace, replicas, replicas - 1);
    }

    private void scaleKroxylicious(String namespace, int replicas, int scaleTo) {
        int numberOfMessages = 10;

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(replicas).log();
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlainWithNoFilters(clusterName, replicas);
        String bootstrap = kroxylicious.getBootstrap();
        int currentReplicas = kroxylicious.getNumberOfReplicas();
        given(currentReplicas).withFailMessage("unexpected deployed replicas").isEqualTo(replicas);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 2);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("And kroxylicious is scaled to {}").addArgument(scaleTo).log();
        kroxylicious.scaleReplicasTo(scaleTo, Duration.ofMinutes(2));
        currentReplicas = kroxylicious.getNumberOfReplicas();
        assertThat(currentReplicas).withFailMessage("unexpected current scaled replicas").isEqualTo(scaleTo);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord<String, String>> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();
        int numOfMessagesReceived = (int) result.stream().filter(c -> c.value().contains(MESSAGE)).count();
        assertThat(numOfMessagesReceived).withFailMessage("expected messages have not been received!").isEqualTo(numberOfMessages);
    }

    /**
     * Sets before all.
     */
    @BeforeAll
    void setupBefore() {
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
            return;
        }
        LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

        Kafka kafka = KafkaTemplates.kafkaPersistentWithKRaftAnnotations(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, 3).build();

        resourceManager.createResourceWithWait(
                KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(BROKER_NODE_NAME, kafka, 3).build(),
                kafka);
    }
}
