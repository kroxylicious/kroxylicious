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
     * Produce messages with kcat.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numberOfMessages the number of messages
     */
    public static void produceMessagesWithKcat(String namespace, String topicName, String bootstrap, String message, int numberOfMessages) {
        KafkaUtils.produceMessageWithKcat(namespace, topicName, bootstrap, message, numberOfMessages);
    }

    /**
     * Produce messages with sarama.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     */
    public static void produceMessagesWithSarama(String namespace, String topicName, String bootstrap) {
        KafkaUtils.produceMessagesWithSarama(namespace, topicName, bootstrap);
    }

    /**
     * Consume messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numberOfMessages the number of messages
     * @param timeout the timeout
     * @return the string
     */
    public static String consumeMessages(String namespace, String topicName, String bootstrap, int numberOfMessages, Duration timeout) {
        return KafkaUtils.consumeMessageWithTestClients(namespace, topicName, bootstrap, numberOfMessages, timeout);
    }

    /**
     * Consume messages with kcat
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param timeout the timeout
     * @return the string
     */
    public static String consumeMessagesWithKcat(String namespace, String topicName, String bootstrap, String message, Duration timeout) {
        return KafkaUtils.consumeMessagesWithKcat(namespace, topicName, bootstrap, message, timeout);
    }

    /**
     * Consume messages with sarama
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param timeout the timeout
     * @return the string
     */
    public static String consumeMessagesWithSarama(String namespace, String topicName, String bootstrap, String message, Duration timeout) {
        return KafkaUtils.consumeMessagesWithSarama(namespace, topicName, bootstrap, message, timeout);
    }

    /**
     * Consume encrypted messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numberOfMessages the number of messages
     * @param timeout the timeout
     * @return the string
     */
    public static String consumeEncryptedMessages(String namespace, String topicName, String bootstrap, int numberOfMessages, Duration timeout) {
        return KafkaUtils.consumeEncryptedMessageWithTestClients(namespace, topicName, bootstrap, numberOfMessages, timeout);
    }
}
