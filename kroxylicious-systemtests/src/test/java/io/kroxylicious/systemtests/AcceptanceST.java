/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.io.FileNotFoundException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.templates.strimzi.KafkaTopicTemplates;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The type Acceptance st.
 */
public class AcceptanceST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(AcceptanceST.class);

    /**
     * Produce and consume message.
     *
     * @param testInfo the test info
     */
    @Test
    public void produceAndConsumeMessage(TestInfo testInfo) throws FileNotFoundException, InterruptedException {
        String topicName = "my-topic";
        String message = "Hello-world";
        int numberOfMessages = 10;
        String consumedMessage = message + " - " + (numberOfMessages - 1);

        LOGGER.info("Given KafkaTopic in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
        resourceManager.createResourceWithWait(testInfo, KafkaTopicTemplates.defaultTopic(Constants.KROXY_DEFAULT_NAMESPACE, "my-cluster", topicName, 1, 1, 1).build());

        LOGGER.info("When the message '{}' is sent to the topic '{}'", message, topicName);
        KafkaUtils.ProduceMessageFromYaml(Constants.KROXY_DEFAULT_NAMESPACE, topicName, message, Constants.KROXY_BOOTSTRAP);
        String result = KafkaUtils.ConsumeMessageFromYaml(Constants.KROXY_DEFAULT_NAMESPACE, topicName, Constants.KROXY_BOOTSTRAP, numberOfMessages, 30000);

        LOGGER.info("Then the message is consumed");
        LOGGER.debug("Received: " + result);
        assertTrue(result.contains(consumedMessage), "'" + consumedMessage + "' message not consumed!");
    }

    // @BeforeAll
    // void setupBefore(TestInfo testInfo) {
    // String clusterName = "my-cluster";
    // LOGGER.info("Deploying Kafka in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
    // resourceManager.createResourceWithWait(testInfo, KafkaTemplates.kafkaPersistent(Constants.KROXY_DEFAULT_NAMESPACE, clusterName, 3, 3).build());
    // }
}
