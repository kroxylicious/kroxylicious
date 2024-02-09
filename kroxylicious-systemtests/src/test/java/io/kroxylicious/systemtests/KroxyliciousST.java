/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.Kafka;

import io.kroxylicious.systemtests.extensions.KroxyliciousExtension;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The type Acceptance st.
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
        String expectedMessage = MESSAGE + " - " + (numberOfMessages - 1);

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, 1);
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlainWithNoFilters(clusterName, 1);
        String bootstrap = kroxylicious.getBootstrap();

        LOGGER.info("And KafkaTopic in {} namespace", namespace);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: " + result);

        assertThat(result).withFailMessage("expected message have not been received!").contains(expectedMessage);
    }

    /**
     * Restart kafka brokers.
     *
     * @param namespace the namespace
     */
    @Test
    void restartKafkaBrokers(String namespace) {
        int numberOfMessages = 25;
        String expectedMessage = MESSAGE + " - " + (numberOfMessages - 1);

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, 1);
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlainWithNoFilters(clusterName, 1);
        String bootstrap = kroxylicious.getBootstrap();

        LOGGER.info("And KafkaTopic in {} namespace", namespace);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 2);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        LOGGER.info("And a kafka broker is restarted");
        KafkaSteps.restartKafkaBroker(clusterName);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(10));
        LOGGER.info("Received: " + result);
        assertThat(result).withFailMessage("expected message have not been received!").contains(expectedMessage);
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
        String expectedMessage = MESSAGE + " - " + (numberOfMessages - 1);

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, replicas);
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlainWithNoFilters(clusterName, replicas);
        String bootstrap = kroxylicious.getBootstrap();
        int currentReplicas = kroxylicious.getNumberOfReplicas();
        assertThat(currentReplicas).withFailMessage("unexpected current replicas").isEqualTo(replicas);

        LOGGER.info("And KafkaTopic in {} namespace", namespace);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 2);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: " + result);
        assertThat(result).withFailMessage("expected message have not been received!").contains(expectedMessage);
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
    }
}
