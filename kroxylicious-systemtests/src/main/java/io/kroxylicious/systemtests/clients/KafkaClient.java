/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

/**
 * The interface Kafka client.
 */
public interface KafkaClient {
    /**
     * Sets the namespace where the kafka client will be deployed in kubernetes.
     *
     * @param namespace the namespace
     * @return the kafka client
     */
    KafkaClient inNamespace(String namespace);

    /**
     * Produce messages.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numOfMessages the num of messages
     */
    void produceMessages(String topicName, String bootstrap, String message, int numOfMessages) throws KubeClusterException;

    /**
     * Consume messages.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numOfMessages the num of messages
     * @param timeout the timeout
     * @return the list of ConsumerRecords
     */
    List<ConsumerRecord<String, String>> consumeMessages(String topicName, String bootstrap, int numOfMessages, Duration timeout);
}
