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
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The type Acceptance st.
 */
@ExtendWith(KroxyliciousExtension.class)
class KroxyliciousST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousST.class);
    private static Kroxylicious kroxylicious;
    private final String clusterName = "my-cluster";
    protected static final String CONTROLLER_NODE_NAME = "controller";
    protected static final String BROKER_NODE_NAME = "kafka";

    /**
     * Produce and consume message.
     *
     * @param namespace the namespace
     */
    @Test
    void produceAndConsumeMessage(String namespace) {
        String topicName = "my-topic";
        String message = "Hello-world";
        int numberOfMessages = 1;
        String consumedMessage = message + " - " + (numberOfMessages - 1);

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, 1);
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlain(clusterName, 1);
        String bootstrap = kroxylicious.getBootstrap();

        LOGGER.info("And KafkaTopic in {} namespace", namespace);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, message, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, message, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: " + result);
        assertThat("'" + consumedMessage + "' message not consumed!", result.contains(consumedMessage));
    }

    /**
     * Restart kafka brokers.
     *
     * @param namespace the namespace
     */
    @Test
    void restartKafkaBrokers(String namespace) {
        String topicName = "my-topic2";
        String message = "Hello-world";
        int numberOfMessages = 25;
        String consumedMessage = message + " - " + (numberOfMessages - 1);

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, 1);
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlain(clusterName, 1);
        String bootstrap = kroxylicious.getBootstrap();

        LOGGER.info("And KafkaTopic in {} namespace", namespace);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 2);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, message, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, message, numberOfMessages);
        LOGGER.info("And a kafka broker is restarted");
        KafkaSteps.restartKafkaBroker(clusterName);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(10));
        LOGGER.info("Received: " + result);
        assertThat("'" + consumedMessage + "' message not consumed!", result.contains(consumedMessage));
    }

    /**
     * Kroxylicious with replicas.
     *
     * @param namespace the namespace
     */
    @Test
    void kroxyWithReplicas(String namespace) {
        String topicName = "my-topic3";
        String message = "Hello-world";
        int numberOfMessages = 3;
        int replicas = 3;
        String consumedMessage = message + " - " + (numberOfMessages - 1);

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, replicas);
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlain(clusterName, replicas);
        String bootstrap = kroxylicious.getBootstrap();
        int currentReplicas = kroxylicious.getNumberOfReplicas();
        assertThat("Current replicas: " + currentReplicas + "; expected: " + replicas, currentReplicas == replicas);

        LOGGER.info("And KafkaTopic in {} namespace", namespace);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 2);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, message, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, message, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: " + result);
        assertThat("'" + consumedMessage + "' message not consumed!", result.contains(consumedMessage));
    }

    /**
     * Sets before all.
     */
    @BeforeAll
    void setupBefore() {
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KROXY_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.warn("Skipping kafka deployment. It is already deployed!");
            return;
        }
        LOGGER.info("Deploying Kafka in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);

        Kafka kafka = KafkaTemplates.kafkaPersistentWithKRaftAnnotations(Constants.KROXY_DEFAULT_NAMESPACE, clusterName, 3).build();

        resourceManager.createResourceWithWait(
                KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(BROKER_NODE_NAME, kafka, 3).build(),
                kafka);
    }
}
