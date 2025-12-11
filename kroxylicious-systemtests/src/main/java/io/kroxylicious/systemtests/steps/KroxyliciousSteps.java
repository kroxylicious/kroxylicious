/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.record.CompressionType;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kroxylicious steps.
 */
public class KroxyliciousSteps {

    private KroxyliciousSteps() {
    }

    /**
     * Gets additional SASL props.
     *
     * @param namespace the namespace
     * @param user the user
     * @param password the password
     * @return  the additional SASL props
     */
    public static Map<String, String> getAdditionalSaslProps(String namespace, String user, String password) {
        return KafkaClients.getKafkaClient().inNamespace(namespace).getAdditionalSaslProps(user, password);
    }

    /**
     * Gets consumer log.
     *
     * @param namespace the namespace
     * @return  the consumer log
     */
    public static String getConsumerLog(String namespace) {
        String podName = KafkaUtils.getPodNameByPrefix(namespace, Constants.KAFKA_CONSUMER_CLIENT_LABEL, Duration.ofSeconds(30));
        return kubeClient().logsInSpecificNamespace(namespace, podName);
    }

    /**
     * Produce messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numberOfMessages the number of messages
     * @return  the exec result
     */
    public static ExecResult produceMessages(String namespace, String topicName, String bootstrap, String message, int numberOfMessages) {
        return KafkaClients.getKafkaClient().inNamespace(namespace).produceMessages(topicName, bootstrap, message, numberOfMessages);
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
     * @return
     */
    public static ExecResult produceMessages(String namespace, String topicName, String bootstrap, String message, @NonNull CompressionType compressionType,
                                       int numberOfMessages) {
        return KafkaClients.getKafkaClient().inNamespace(namespace).produceMessages(topicName, bootstrap, message, compressionType, numberOfMessages);
    }

    /**
     * Produce messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numberOfMessages the number of messages
     * @param additionalConfig the additional config
     * @return
     */
    public static ExecResult produceMessages(String namespace, String topicName, String bootstrap, String message, int numberOfMessages,
                                             Map<String, String> additionalConfig) {
        return KafkaClients.getKafkaClient().inNamespace(namespace).produceMessages(topicName, bootstrap, message, null, numberOfMessages, additionalConfig);
    }

    /**
     * Consume messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numberOfMessages the number of messages
     * @param timeout the timeout
     * @return  the list of ConsumerRecords
     */
    public static List<ConsumerRecord> consumeMessages(String namespace, String topicName, String bootstrap, int numberOfMessages, Duration timeout) {
        return KafkaClients.getKafkaClient().inNamespace(namespace).consumeMessages(topicName, bootstrap, numberOfMessages, timeout, Map.of());
    }

    /**
     * Consume messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numberOfMessages the number of messages
     * @param timeout the timeout
     * @param additionalConfig the additional config
     * @return  the list of ConsumerRecords
     */
    public static List<ConsumerRecord> consumeMessages(String namespace, String topicName, String bootstrap, int numberOfMessages, Duration timeout,
                                                       Map<String, String> additionalConfig) {
        return KafkaClients.getKafkaClient().inNamespace(namespace).consumeMessages(topicName, bootstrap, numberOfMessages, timeout, additionalConfig);
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
