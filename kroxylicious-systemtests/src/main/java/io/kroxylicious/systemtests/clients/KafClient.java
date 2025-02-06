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
import java.util.stream.Stream;

import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClientException;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.clients.records.KafConsumerRecord;
import io.kroxylicious.systemtests.enums.KafkaClientType;
import io.kroxylicious.systemtests.templates.testclients.TestClientsJobTemplates;
import io.kroxylicious.systemtests.utils.KafkaUtils;
import io.kroxylicious.systemtests.utils.TestUtils;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.getInstance;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Kaf client (sarama client based CLI).
 */
public class KafClient implements KafkaClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafClient.class);
    private static final TypeReference<KafConsumerRecord> VALUE_TYPE_REF = new TypeReference<>() {
    };
    private String deployNamespace;

    /**
     * Instantiates a new Kaf client.
     */
    public KafClient() {
        this.deployNamespace = kubeClient().getNamespace();
    }

    @Override
    public KafkaClient inNamespace(String namespace) {
        this.deployNamespace = namespace;
        return this;
    }

    @Override
    public void produceMessages(String topicName, String bootstrap, String message, @Nullable String messageKey, int numOfMessages) {
        LOGGER.atInfo().setMessage("Producing messages in '{}' topic using kaf").addArgument(topicName).log();
        final Optional<String> recordKey = Optional.ofNullable(messageKey);
        String name = Constants.KAFKA_PRODUCER_CLIENT_LABEL + "-kaf-" + TestUtils.getRandomPodNameSuffix();
        String jsonOverrides = getInstance().isOpenshift() ? TestUtils.getJsonFileContent("nonJVMClient_openshift.json").replace("%NAME%", name) : "";

        List<String> executableCommand = new ArrayList<>(List.of(cmdKubeClient(deployNamespace).toString(), "run", "-i",
                "-n", deployNamespace, name,
                "--image=" + Constants.KAF_CLIENT_IMAGE,
                "--override-type=strategic",
                "--overrides=" + jsonOverrides,
                "--", "kaf", "-n", String.valueOf(numOfMessages), "-b", bootstrap));
        recordKey.ifPresent(key -> {
            executableCommand.add("--key");
            executableCommand.add(key);
        });
        executableCommand.addAll(List.of("produce", topicName));

        KafkaUtils.produceMessagesWithCmd(deployNamespace, executableCommand, message, name, KafkaClientType.KAF.name().toLowerCase());
    }

    @Override
    public List<ConsumerRecord> consumeMessages(String topicName, String bootstrap, int numOfMessages, Duration timeout) {
        LOGGER.atInfo().log("Consuming messages using kaf");
        String name = Constants.KAFKA_CONSUMER_CLIENT_LABEL + "-kaf-" + TestUtils.getRandomPodNameSuffix();
        List<String> args = List.of("kaf", "-b", bootstrap, "consume", topicName, "--output", "json");
        Job goClientJob = TestClientsJobTemplates.defaultKafkaGoConsumerJob(name, args).build();
        String podName = KafkaUtils.createJob(deployNamespace, name, goClientJob);
        String log = waitForConsumer(podName, numOfMessages, timeout);
        LOGGER.atInfo().setMessage("Log: {}").addArgument(log).log();
        List<String> logRecords = extractRecordLinesFromLog(log);
        return getConsumerRecords(topicName, logRecords);
    }

    private String waitForConsumer(String podName, int numOfMessages, Duration timeout) {
        String log;
        try {
            log = await().alias("Consumer waiting to receive messages")
                    .ignoreException(KubernetesClientException.class)
                    .atMost(timeout)
                    .until(() -> {
                        if (kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName).get() != null) {
                            return kubeClient().logsInSpecificNamespace(deployNamespace, podName);
                        }
                        return null;
                    }, m -> getNumberOfJsonMessages(m) >= numOfMessages);
        }
        catch (ConditionTimeoutException e) {
            log = kubeClient().logsInSpecificNamespace(deployNamespace, podName);
            LOGGER.atInfo().setMessage("Timeout! Received: {}").addArgument(log).log();
            if (!kubeClient().isPodRunSucceeded(deployNamespace, podName)) {
                throw new IllegalStateException("Error in consumer: ", e);
            }
        }
        return log;
    }

    private int getNumberOfJsonMessages(String log) {
        return log == null ? 0 : extractRecordLinesFromLog(log).size();
    }

    private List<String> extractRecordLinesFromLog(String log) {
        return Stream.of(log.split("\n")).filter(TestUtils::isValidJson).toList();
    }

    private List<ConsumerRecord> getConsumerRecords(String topicName, List<String> logRecords) {
        List<ConsumerRecord> records = new ArrayList<>();
        for (String logRecord : logRecords) {
            KafConsumerRecord kafConsumerRecord = ConsumerRecord.parseFromJsonString(VALUE_TYPE_REF, logRecord);
            kafConsumerRecord.setTopic(topicName);
            records.add(kafConsumerRecord);
        }

        return records;
    }
}
