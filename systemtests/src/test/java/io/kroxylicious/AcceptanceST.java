/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.executor.ExecResult;
import io.kroxylicious.templates.strimzi.KafkaTopicTemplates;
import io.kroxylicious.utils.KafkaUtils;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AcceptanceST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(AcceptanceST.class);

    @Test
    void testMe(TestInfo testInfo) {
        String topicName = "my-topic";
        String message = "Hello, world!";

        LOGGER.info("Deploying KafkaTopic in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
        resourceManager.createResourceWithWait(testInfo, KafkaTopicTemplates.defaultTopic(Constants.KROXY_DEFAULT_NAMESPACE, "my-cluster", topicName, 1, 1, 1).build());

        KafkaUtils.ProduceMessage(topicName, message);
        ExecResult result = KafkaUtils.ConsumeMessage(topicName, 10000);

        assertTrue(result.out().contains(message));
    }

    @Test
    void anotherTest(TestInfo testInfo) {
        String topicName = "my-topic-2";

        LOGGER.info("Deploying KafkaTopic in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
        resourceManager.createResourceWithWait(testInfo, KafkaTopicTemplates.defaultTopic(Constants.KROXY_DEFAULT_NAMESPACE, "my-cluster", topicName, 1, 1, 1).build());

        LOGGER.info("End of example test 2, now should be everything deleted");
    }

    // @BeforeAll
    // void setupBefore(TestInfo testInfo) {
    // String clusterName = "my-cluster";
    // LOGGER.info("Deploying Kafka in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);
    // resourceManager.createResourceWithWait(testInfo, KafkaTemplates.kafkaPersistent(Constants.KROXY_DEFAULT_NAMESPACE, clusterName, 3, 3).build());
    // }
}
