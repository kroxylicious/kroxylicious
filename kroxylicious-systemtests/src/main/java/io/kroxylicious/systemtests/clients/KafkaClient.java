/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.common.record.CompressionType;

import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

import edu.umd.cs.findbugs.annotations.Nullable;

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
    default void produceMessages(String topicName, String bootstrap, String message, int numOfMessages) throws KubeClusterException {
        produceMessages(topicName, bootstrap, message, null, CompressionType.NONE.name, numOfMessages);
    }

    /**
     * Produce messages.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param compressionType the compression type
     * @param numOfMessages the num of messages
     * @throws KubeClusterException the kube cluster exception
     */
    default void produceMessages(String topicName, String bootstrap, String message, CompressionType compressionType, int numOfMessages) throws KubeClusterException {
        produceMessages(topicName, bootstrap, message, null, compressionType.name, numOfMessages);
    }

    /**
     * Produce messages.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param messageKey optional record key for the message. <code>null</code> means don't specify a key
     * @param compressionType the compression type
     * @param numOfMessages the num of messages
     * @throws KubeClusterException the kube cluster exception
     */
    void produceMessages(String topicName, String bootstrap, String message, @Nullable String messageKey, String compressionType, int numOfMessages)
            throws KubeClusterException;

    /**
     * Consume messages.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numOfMessages the num of messages
     * @param timeout the timeout
     * @return the list of ConsumerRecords
     */
    List<ConsumerRecord> consumeMessages(String topicName, String bootstrap, int numOfMessages, Duration timeout);
}
