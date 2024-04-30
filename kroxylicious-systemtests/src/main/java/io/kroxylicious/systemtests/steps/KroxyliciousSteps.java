/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.time.Duration;

import io.kroxylicious.systemtests.utils.KafkaUtils;

/**
 * The type Kroxylicious steps.
 */
public class KroxyliciousSteps {

    private KroxyliciousSteps() {
    }

    /**
     * Produce messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numberOfMessages the number of messages
     */
    public static void produceMessages(String namespace, String topicName, String bootstrap, String message, int numberOfMessages) {
        KafkaUtils.produceMessageWithTestClients(namespace, topicName, bootstrap, message, numberOfMessages);
    }

    /**
     * Consume messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numberOfMessages the number of messages
     * @param timeout the timeout
     * @return the string
     */
    public static String consumeMessages(String namespace, String topicName, String bootstrap, String message, int numberOfMessages, Duration timeout) {
        return KafkaUtils.consumeMessageWithTestClients(namespace, topicName, bootstrap, message, numberOfMessages, timeout);
    }

    /**
     * Kroxylicious will decrypt the message so to consume an encrypted message we need to read from the underlying Kafka cluster.
     *
     * @param clientNamespace where to run the client job
     * @param topicName the topic name
     * @param kafkaClusterName the name of the kafka cluster to read from
     * @param kafkaNamespace the namespace in which the broker is operating
     * @param numberOfMessages the number of messages
     * @param timeout maximum time to wait for the expectedMessage to appear
     * @param expectedMessage the message to check for.
     * @return the string
     */
    public static String consumeMessageFromKafkaCluster(String clientNamespace, String topicName, String kafkaClusterName, String kafkaNamespace, int numberOfMessages,
                                                        Duration timeout, String expectedMessage) {
        String kafkaBootstrap = kafkaClusterName + "-kafka-bootstrap." + kafkaNamespace + ".svc.cluster.local:9092";
        return KafkaUtils.consumeMessageWithTestClients(clientNamespace, topicName, kafkaBootstrap, expectedMessage, numberOfMessages, timeout);
    }
}
