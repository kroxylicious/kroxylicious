/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

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
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numberOfMessages the number of messages
     */
    public static void produceMessages(String namespace, String topicName, String bootstrap, String message, int numberOfMessages) {
        KafkaUtils.produceMessageWithTestClients(namespace, topicName, bootstrap, message, numberOfMessages);
    }

    /**
     * Consume messages string.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numberOfMessages the number of messages
     * @param timeoutMillis the timeout millis
     * @return the string
     */
    public static String consumeMessages(String namespace, String topicName, String bootstrap, int numberOfMessages, long timeoutMillis) {
        return KafkaUtils.ConsumeMessageWithTestClients(namespace, topicName, bootstrap, numberOfMessages, timeoutMillis);
    }
}
