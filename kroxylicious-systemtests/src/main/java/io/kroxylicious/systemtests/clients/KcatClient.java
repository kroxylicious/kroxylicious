/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;

import io.fabric8.kubernetes.api.model.batch.v1.Job;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.clients.records.BaseConsumerRecord;
import io.kroxylicious.systemtests.clients.records.KcatConsumerRecord;
import io.kroxylicious.systemtests.enums.KafkaClientType;
import io.kroxylicious.systemtests.templates.testclients.TestClientsJobTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kcat client (librdkafka client based CLI).
 */
public class KcatClient implements KafkaClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KcatClient.class);
    private String deployNamespace;

    /**
     * Instantiates a new Kcat client.
     */
    public KcatClient() {
        this.deployNamespace = kubeClient().getNamespace();
    }

    @Override
    public KafkaClient inNamespace(String namespace) {
        this.deployNamespace = namespace;
        return this;
    }

    @Override
    public void produceMessages(String topicName, String bootstrap, String message, @Nullable String messageKey, int numOfMessages) {
        final Optional<String> recordKey = Optional.ofNullable(messageKey);

        StringBuilder msg = new StringBuilder();
        for (int i = 0; i < numOfMessages; i++) {
            recordKey.ifPresent(k -> msg.append(k).append(":"));
            msg.append(message)
                    .append(" - ")
                    .append(i)
                    .append("\n");
        }

        LOGGER.atInfo().setMessage("Producing messages in '{}' topic using kcat").addArgument(topicName).log();
        String name = Constants.KAFKA_PRODUCER_CLIENT_LABEL + "-kcat";
        List<String> executableCommand = new ArrayList<>(List.of(cmdKubeClient(deployNamespace).toString(), "run", "-i",
                "-n", deployNamespace, name,
                "--image=" + Constants.KCAT_CLIENT_IMAGE,
                "--", "-b", bootstrap, "-l", "-t", topicName, "-P"));
        recordKey.ifPresent(ignored -> executableCommand.add("-K :"));

        KafkaUtils.produceMessagesWithCmd(deployNamespace, executableCommand, String.valueOf(msg), name, KafkaClientType.KCAT.name().toLowerCase());
    }

    @Override
    public List<ConsumerRecord<String, String>> consumeMessages(String topicName, String bootstrap, int numOfMessages, Duration timeout) {
        LOGGER.atInfo().log("Consuming messages using kcat");
        String name = Constants.KAFKA_CONSUMER_CLIENT_LABEL + "-kcat";
        List<String> args = List.of("-b", bootstrap, "-K ,", "-t", topicName, "-C", "-c", String.valueOf(numOfMessages), "-e", "-J");
        Job kCatClientJob = TestClientsJobTemplates.defaultKcatJob(name, args).build();
        String podName = KafkaUtils.createJob(deployNamespace, name, kCatClientJob);
        String log = waitForConsumer(deployNamespace, podName, timeout);
        LOGGER.atInfo().log(log);
        List<String> logRecords = List.of(log.split("\n"));
        return getConsumerRecords(logRecords);
    }

    private String waitForConsumer(String namespace, String podName, Duration timeout) {
        DeploymentUtils.waitForPodRunSucceeded(namespace, podName, timeout);
        return kubeClient().logsInSpecificNamespace(namespace, podName);
    }

    private List<ConsumerRecord<String, String>> getConsumerRecords(List<String> logRecords) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (String logRecord : logRecords) {
            KcatConsumerRecord kcatConsumerRecord = BaseConsumerRecord.parseFromJsonString(new TypeReference<>() {
            }, logRecord);
            if (kcatConsumerRecord != null) {
                ConsumerRecord<String, String> consumerRecord = kcatConsumerRecord.toConsumerRecord();
                records.add(consumerRecord);
            }
        }

        return records;
    }
}
