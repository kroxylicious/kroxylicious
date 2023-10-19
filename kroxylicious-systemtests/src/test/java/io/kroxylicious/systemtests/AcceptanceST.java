/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.io.IOException;
import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxySteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The type Acceptance st.
 */
class AcceptanceST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(AcceptanceST.class);
    private final String clusterName = "my-cluster";

    /**
     * Produce and consume message.
     *
     * @param testInfo the test info
     * @throws IOException the io exception
     */
    @Test
    void produceAndConsumeMessage(TestInfo testInfo) throws IOException {
        String topicName = "my-topic";
        String message = "Hello-world";
        int numberOfMessages = 1;
        String consumedMessage = message + " - " + (numberOfMessages - 1);

        // start kroxy
        LOGGER.info("Given Kroxy in {} namespace with {} replicas", Constants.KROXY_DEFAULT_NAMESPACE, 1);
        kroxylicious = new Kroxylicious(Constants.KROXY_DEFAULT_NAMESPACE);
        kroxylicious.deployPortPerBrokerPlain(testInfo, 1);

        LOGGER.info("And KafkaTopic in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
        KafkaSteps.createTopic(testInfo, topicName, clusterName, 1, 1, 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, message, topicName);
        KroxySteps.produceMessages(topicName, Constants.KROXY_BOOTSTRAP, message, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxySteps.consumeMessages(topicName, Constants.KROXY_BOOTSTRAP, numberOfMessages, Duration.ofMinutes(2).toMillis());
        LOGGER.info("Received: " + result);
        assertTrue(result.contains(consumedMessage), "'" + consumedMessage + "' message not consumed!");
    }

    /**
     * Restart kafka brokers.
     *
     * @param testInfo the test info
     * @throws IOException the io exception
     */
    @Test
    void restartKafkaBrokers(TestInfo testInfo) throws IOException {
        String topicName = "my-topic2";
        String message = "Hello-world";
        int numberOfMessages = 25;
        String consumedMessage = message + " - " + (numberOfMessages - 1);

        // start kroxy
        LOGGER.info("Given Kroxy in {} namespace with {} replicas", Constants.KROXY_DEFAULT_NAMESPACE, 1);
        kroxylicious = new Kroxylicious(Constants.KROXY_DEFAULT_NAMESPACE);
        kroxylicious.deployPortPerBrokerPlain(testInfo, 1);

        LOGGER.info("And KafkaTopic in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
        KafkaSteps.createTopic(testInfo, topicName, clusterName, 3, 1, 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, message, topicName);
        KroxySteps.produceMessages(topicName, Constants.KROXY_BOOTSTRAP, message, numberOfMessages);
        LOGGER.info("And a kafka broker is restarted");
        KafkaSteps.restartKakfaBroker(clusterName);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxySteps.consumeMessages(topicName, Constants.KROXY_BOOTSTRAP, numberOfMessages, Duration.ofMinutes(10).toMillis());
        LOGGER.info("Received: " + result);
        assertTrue(result.contains(consumedMessage), "'" + consumedMessage + "' message not consumed!");
    }

    /**
     * Kroxy with replicas.
     *
     * @param testInfo the test info
     * @throws IOException the io exception
     */
    @Test
    void kroxyWithReplicas(TestInfo testInfo) throws IOException {
        String topicName = "my-topic3";
        String message = "Hello-world";
        int numberOfMessages = 5;
        int replicas = 3;
        String consumedMessage = message + " - " + (numberOfMessages - 1);

        // start kroxy
        LOGGER.info("Given Kroxy in {} namespace with {} replicas", Constants.KROXY_DEFAULT_NAMESPACE, replicas);
        kroxylicious = new Kroxylicious(Constants.KROXY_DEFAULT_NAMESPACE);
        kroxylicious.deployPortPerBrokerPlain(testInfo, replicas);
        int currentReplicas = kroxylicious.getNumberOfReplicas();
        assertEquals(currentReplicas, replicas, "Current replicas: " + currentReplicas + "; expected: " + replicas);

        LOGGER.info("And KafkaTopic in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
        KafkaSteps.createTopic(testInfo, topicName, clusterName, 3, 1, 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, message, topicName);
        KroxySteps.produceMessages(topicName, Constants.KROXY_BOOTSTRAP, message, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxySteps.consumeMessages(topicName, Constants.KROXY_BOOTSTRAP, numberOfMessages, Duration.ofMinutes(2).toMillis());
        LOGGER.info("Received: " + result);
        assertTrue(result.contains(consumedMessage), "'" + consumedMessage + "' message not consumed!");
    }

    /**
     * Sets before.
     *
     * @param testInfo the test info
     */
    @BeforeAll
    void setupBefore(TestInfo testInfo) {
        LOGGER.info("Deploying Kafka in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
        resourceManager.createResourceWithWait(testInfo, KafkaTemplates.kafkaPersistent(Constants.KROXY_DEFAULT_NAMESPACE, clusterName, 3, 3).build());
    }
}
