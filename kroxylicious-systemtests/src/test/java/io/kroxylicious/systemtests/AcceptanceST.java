/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.installation.kroxy.Kroxy;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxySteps;
import io.kroxylicious.systemtests.templates.KafkaTemplates;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The type Acceptance st.
 */
public class AcceptanceST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(AcceptanceST.class);
    private static final Path projectPath = Path.of(System.getProperty("user.dir")).getParent();
    private final String clusterName = "my-cluster";

    /**
     * Produce and consume message.
     *
     * @param testInfo the test info
     */
    @Test
    public void produceAndConsumeMessage(TestInfo testInfo) throws IOException {
        String topicName = "my-topic";
        String message = "Hello-world";
        int numberOfMessages = 10;
        String consumedMessage = message + " - " + (numberOfMessages - 1);

        // start kroxy
        kroxy = new Kroxy(Constants.KROXY_DEFAULT_NAMESPACE);
        kroxy.deployPortPerBrokerPlain(testInfo);

        LOGGER.info("Given KafkaTopic in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
        KafkaSteps.createTopic(testInfo, topicName, clusterName, 1, 1, 1);

        LOGGER.info("When the message '{}' is sent to the topic '{}'", message, topicName);
        KroxySteps.produceMessages(topicName, message, numberOfMessages);

        LOGGER.info("Then the message is consumed");
        String result = KroxySteps.consumeMessages(topicName, numberOfMessages, Duration.ofMinutes(1).toMillis());
        LOGGER.debug("Received: " + result);
        assertTrue(result.contains(consumedMessage), "'" + consumedMessage + "' message not consumed!");
    }

    @BeforeAll
    void setupBefore(TestInfo testInfo) {
        LOGGER.info("Deploying Kafka in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
        resourceManager.createResourceWithWait(testInfo, KafkaTemplates.kafkaPersistent(Constants.KROXY_DEFAULT_NAMESPACE, clusterName, 3, 3).build());
    }
}
