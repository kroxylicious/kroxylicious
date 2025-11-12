/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.common.record.CompressionType;

import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;

import edu.umd.cs.findbugs.annotations.NonNull;

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
        KafkaClients.getKafkaClient().inNamespace(namespace).produceMessages(topicName, bootstrap, message, numberOfMessages);
    }

    /**
     * Produce messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param compressionType the compression type
     * @param numberOfMessages the number of messages
     */
    public static void produceMessages(String namespace, String topicName, String bootstrap, String message, @NonNull CompressionType compressionType,
                                       int numberOfMessages) {
        KafkaClients.getKafkaClient().inNamespace(namespace).produceMessages(topicName, bootstrap, message, compressionType, numberOfMessages);
    }

    /**
     * Consume messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numberOfMessages the number of messages
     * @param timeout the timeout
     * @return the list of ConsumerRecords
     */
    public static List<ConsumerRecord> consumeMessages(String namespace, String topicName, String bootstrap, int numberOfMessages, Duration timeout) {
        return KafkaClients.getKafkaClient().inNamespace(namespace).consumeMessages(topicName, bootstrap, numberOfMessages, timeout);
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
     * @return the list of consumer records
     */
    public static List<ConsumerRecord> consumeMessageFromKafkaCluster(String clientNamespace, String topicName, String kafkaClusterName,
                                                                      String kafkaNamespace, int numberOfMessages,
                                                                      Duration timeout) {
        String kafkaBootstrap = kafkaClusterName + "-kafka-bootstrap." + kafkaNamespace + ".svc.cluster.local:9092";
        return consumeMessages(clientNamespace, topicName, kafkaBootstrap, numberOfMessages, timeout);
    }
}
