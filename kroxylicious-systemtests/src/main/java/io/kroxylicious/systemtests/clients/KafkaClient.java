/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

import edu.umd.cs.findbugs.annotations.NonNull;
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

    default Map<String, String> getAdditionalSaslProps(String user, String password) {
        return Map.of("sasl.username", user, "sasl.password", password, SaslConfigs.SASL_MECHANISM,
                ScramMechanism.SCRAM_SHA_512.mechanismName(), CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
    }

    /**
     * Produce messages.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numOfMessages the num of messages
     * @throws KubeClusterException the kube cluster exception
     */
    default void produceMessages(String topicName, String bootstrap, String message, int numOfMessages) throws KubeClusterException {
        produceMessages(topicName, bootstrap, message, null, numOfMessages, Map.of());
    }

    /**
     * Produce messages.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param messageKey optional record key for the message. <code>null</code> means don't specify a key
     * @param numOfMessages the num of messages
     * @throws KubeClusterException the kube cluster exception
     */
    default void produceMessages(String topicName, String bootstrap, String message, @Nullable String messageKey, int numOfMessages)
            throws KubeClusterException {
        produceMessages(topicName, bootstrap, message, messageKey, numOfMessages, Map.of());
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
    default void produceMessages(String topicName, String bootstrap, String message, @NonNull CompressionType compressionType, int numOfMessages)
            throws KubeClusterException {
        produceMessages(topicName, bootstrap, message, null, numOfMessages, Map.of(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType.name));
    }

    /**
     * Produce messages.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param messageKey optional record key for the message. <code>null</code> means don't specify a key
     * @param numOfMessages the num of messages
     * @param additionalConfig the additional config
     * @throws KubeClusterException the kube cluster exception
     */
    void produceMessages(String topicName, String bootstrap, String message, @Nullable String messageKey, int numOfMessages, Map<String, String> additionalConfig)
            throws KubeClusterException;

    /**
     * Consume messages.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numOfMessages the num of messages
     * @param timeout the timeout
     * @return  the list of ConsumerRecords
     * @throws KubeClusterException the kube cluster exception
     */
    default List<ConsumerRecord> consumeMessages(String topicName, String bootstrap, int numOfMessages, Duration timeout) throws KubeClusterException {
        return consumeMessages(topicName, bootstrap, numOfMessages, timeout, Map.of());
    }

    /**
     * Consume messages.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numOfMessages the num of messages
     * @param timeout the timeout
     * @param additionalConfig the additional config
     * @return  the list of ConsumerRecords
     */
    List<ConsumerRecord> consumeMessages(String topicName, String bootstrap, int numOfMessages, Duration timeout, Map<String, String> additionalConfig);
}
