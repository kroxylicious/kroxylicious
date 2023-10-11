/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.io.IOException;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.utils.KafkaUtils;

/**
 * The type Kroxy steps.
 */
public class KroxySteps {

    /**
     * Produce messages.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numberOfMessages the number of messages
     * @throws IOException the io exception
     */
    public static void produceMessages(String topicName, String bootstrap, String message, int numberOfMessages) throws IOException {
        KafkaUtils.produceMessageWithTestClients(Constants.KROXY_DEFAULT_NAMESPACE, topicName, bootstrap, message, numberOfMessages);
    }

    /**
     * Consume messages string.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numberOfMessages the number of messages
     * @param timeoutMillis the timeout millis
     * @return the string
     * @throws IOException the io exception
     */
    public static String consumeMessages(String topicName, String bootstrap, int numberOfMessages, long timeoutMillis) throws IOException {
        return KafkaUtils.ConsumeMessageWithTestClients(Constants.KROXY_DEFAULT_NAMESPACE, topicName, bootstrap, numberOfMessages, timeoutMillis);
    }
}
