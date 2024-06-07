/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClientException;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.clients.records.ClientConsumerRecord;
import io.kroxylicious.systemtests.templates.testclients.TestClientsJobTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Strimzi Test client (java client based CLI).
 */
public class StrimziTestClient implements KafkaClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziTestClient.class);
    private String deployNamespace;

    /**
     * Instantiates a new Strimzi Test client.
     */
    public StrimziTestClient() {
        this.deployNamespace = kubeClient().getNamespace();
    }

    @Override
    public KafkaClient inNamespace(String namespace) {
        this.deployNamespace = namespace;
        return this;
    }

    @Override
    public void produceMessages(String topicName, String bootstrap, String message, @Nullable String messageKey, int numOfMessages) {
        LOGGER.atInfo().log("Producing messages using Strimzi Test Client");
        String name = Constants.KAFKA_PRODUCER_CLIENT_LABEL;
        Job testClientJob = TestClientsJobTemplates.defaultTestClientProducerJob(name, bootstrap, topicName, numOfMessages, message, messageKey).build();
        KafkaUtils.produceMessages(deployNamespace, topicName, name, testClientJob);
        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
        String log = waitForProducer(deployNamespace, podName, Duration.ofSeconds(60));
        LOGGER.atInfo().setMessage("client producer log: {}").addArgument(log).log();
    }

    private static String waitForProducer(String namespace, String podName, Duration timeout) {
        String log;
        try {
            log = await().alias("Consumer waiting to receive messages")
                    .ignoreException(KubernetesClientException.class)
                    .atMost(timeout)
                    .until(() -> {
                        if (kubeClient().getClient().pods().inNamespace(namespace).withName(podName).get() != null) {
                            return kubeClient().logsInSpecificNamespace(namespace, podName);
                        }
                        return null;
                    }, m -> m.contains("Sending message:"));
        }
        catch (ConditionTimeoutException e) {
            log = kubeClient().logsInSpecificNamespace(namespace, podName);
            LOGGER.atInfo().setMessage("Timeout! Unable to produce the messages: {}").addArgument(log).log();
        }
        return log;
    }

    @Override
    public List<ConsumerRecord<String, String>> consumeMessages(String topicName, String bootstrap, int numOfMessages, Duration timeout) {
        LOGGER.atInfo().log("Consuming messages using Strimzi Test Client");
        String name = Constants.KAFKA_CONSUMER_CLIENT_LABEL;
        Job testClientJob = TestClientsJobTemplates.defaultTestClientConsumerJob(name, bootstrap, topicName, numOfMessages).build();
        String podName = KafkaUtils.createJob(deployNamespace, name, testClientJob);
        String log = waitForConsumer(deployNamespace, podName, timeout);
        LOGGER.atInfo().log(log);
        List<String> logRecords = extractRecordLinesFromLog(log);
        return getConsumerRecords(logRecords);
    }

    private String waitForConsumer(String namespace, String podName, Duration timeout) {
        DeploymentUtils.waitForPodRunSucceeded(namespace, podName, timeout);
        return kubeClient().logsInSpecificNamespace(namespace, podName);
    }

    private List<ConsumerRecord<String, String>> getConsumerRecords(List<String> logRecords) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (String logRecord : logRecords) {
            ClientConsumerRecord clientConsumerRecord = ClientConsumerRecord.parseFromJsonString(logRecord);
            if (clientConsumerRecord != null) {
                ConsumerRecord<String, String> consumerRecord = clientConsumerRecord.toConsumerRecord();
                records.add(consumerRecord);
            }
        }

        return records;
    }

    private List<String> extractRecordLinesFromLog(String log) {
        List<String> records = new ArrayList<>();
        String stringToSeek = "Received message:";

        List<String> receivedMessages = Stream.of(log.split("\n")).filter(l -> l.contains(stringToSeek)).toList();
        for (String receivedMessage : receivedMessages) {
            records.add(receivedMessage.split(stringToSeek)[1].trim());
        }

        return records;
    }
}
