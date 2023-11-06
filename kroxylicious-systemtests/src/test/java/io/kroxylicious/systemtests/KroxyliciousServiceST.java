/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.extensions.KroxyliciousExtension;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousService;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxySteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The type Acceptance st.
 */
class KroxyliciousServiceST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousServiceST.class);
    private static KroxyliciousService kroxyliciousService;
    private final String clusterName = "my-cluster";

    /**
     * Produce and consume message.
     *
     * @param namespace the namespace
     */
    @Test
    @ExtendWith(KroxyliciousExtension.class)
    void produceAndConsumeMessage(String namespace) {
        String topicName = "my-topic";
        String message = "Hello-world";
        int numberOfMessages = 1;
        String consumedMessage = message + " - " + (numberOfMessages - 1);

        // start kroxy
        LOGGER.info("Given Kroxy in {} namespace with {} replicas", namespace, 1);
        kroxyliciousService = new KroxyliciousService(namespace);
        kroxyliciousService.deployPortPerBrokerPlain(clusterName, 1);

        LOGGER.info("And KafkaTopic in {} namespace", namespace);
        KafkaSteps.createTopic(topicName, clusterName, namespace, 1, 1, 1);

        String bootstrap = kroxyliciousService.getBootstrap();

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, message, topicName);
        KroxySteps.produceMessages(namespace, topicName, bootstrap, message, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxySteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2).toMillis());
        LOGGER.info("Received: " + result);
        assertThat("'" + consumedMessage + "' message not consumed!", result.contains(consumedMessage));
    }

    /**
     * Restart kafka brokers.
     *
     * @param namespace the namespace
     */
    @Test
    @ExtendWith(KroxyliciousExtension.class)
    void restartKafkaBrokers(String namespace) {
        String topicName = "my-topic2";
        String message = "Hello-world";
        int numberOfMessages = 20;
        String consumedMessage = message + " - " + (numberOfMessages - 1);

        // start kroxy
        LOGGER.info("Given Kroxy in {} namespace with {} replicas", namespace, 1);
        kroxyliciousService = new KroxyliciousService(namespace);
        kroxyliciousService.deployPortPerBrokerPlain(clusterName, 1);
        String bootstrap = kroxyliciousService.getBootstrap();

        LOGGER.info("And KafkaTopic in {} namespace", namespace);
        KafkaSteps.createTopic(topicName, clusterName, namespace, 3, 1, 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, message, topicName);
        KroxySteps.produceMessages(namespace, topicName, bootstrap, message, numberOfMessages);
        LOGGER.info("And a kafka broker is restarted");
        KafkaSteps.restartKafkaBroker(clusterName);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxySteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(10).toMillis());
        LOGGER.info("Received: " + result);
        assertThat("'" + consumedMessage + "' message not consumed!", result.contains(consumedMessage));
    }

    /**
     * Kroxy with replicas.
     *
     * @param namespace the namespace
     */
    @Test
    @ExtendWith(KroxyliciousExtension.class)
    void kroxyWithReplicas(String namespace) {
        String topicName = "my-topic3";
        String message = "Hello-world";
        int numberOfMessages = 3;
        int replicas = 3;
        String consumedMessage = message + " - " + (numberOfMessages - 1);

        // start kroxy
        LOGGER.info("Given Kroxy in {} namespace with {} replicas", namespace, replicas);
        kroxyliciousService = new KroxyliciousService(namespace);
        kroxyliciousService.deployPortPerBrokerPlain(clusterName, replicas);
        String bootstrap = kroxyliciousService.getBootstrap();
        int currentReplicas = kroxyliciousService.getNumberOfReplicas();
        assertThat("Current replicas: " + currentReplicas + "; expected: " + replicas, currentReplicas == replicas);

        LOGGER.info("And KafkaTopic in {} namespace", namespace);
        KafkaSteps.createTopic(topicName, clusterName, namespace, 3, 1, 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, message, topicName);
        KroxySteps.produceMessages(namespace, topicName, bootstrap, message, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxySteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2).toMillis());
        LOGGER.info("Received: " + result);
        assertThat("'" + consumedMessage + "' message not consumed!", result.contains(consumedMessage));
    }

    /**
     * Sets before all.
     */
    @BeforeAll
    void setupBefore() {
        LOGGER.info("Deploying Kafka in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(Constants.KROXY_DEFAULT_NAMESPACE, clusterName, 3, 3).build());
    }
}
