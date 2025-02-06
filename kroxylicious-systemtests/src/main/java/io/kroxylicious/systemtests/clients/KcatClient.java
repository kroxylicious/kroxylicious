/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.batch.v1.Job;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.clients.records.KcatConsumerRecord;
import io.kroxylicious.systemtests.enums.KafkaClientType;
import io.kroxylicious.systemtests.templates.testclients.TestClientsJobTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.KafkaUtils;
import io.kroxylicious.systemtests.utils.TestUtils;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.getInstance;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kcat client (librdkafka client based CLI).
 */
public class KcatClient implements KafkaClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KcatClient.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<KcatConsumerRecord> VALUE_TYPE_REF = new TypeReference<>() {
    };
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
        String name = Constants.KAFKA_PRODUCER_CLIENT_LABEL + "-kcat-" + TestUtils.getRandomPodNameSuffix();
        String jsonOverrides = getInstance().isOpenshift() ? TestUtils.getJsonFileContent("nonJVMClient_overrides.json") : "";

        List<String> executableCommand = new ArrayList<>(List.of(cmdKubeClient(deployNamespace).toString(), "run", "-i",
                "-n", deployNamespace, name,
                "--image=" + Constants.KCAT_CLIENT_IMAGE,
                "--override-type=strategic",
                "--overrides=" + jsonOverrides.replace("%NAME%", name),
                "--", "-b", bootstrap, "-l", "-t", topicName, "-P"));
        recordKey.ifPresent(ignored -> executableCommand.add("-K :"));

        KafkaUtils.produceMessagesWithCmd(deployNamespace, executableCommand, String.valueOf(msg), name, KafkaClientType.KCAT.name().toLowerCase());
    }

    @Override
    public List<ConsumerRecord> consumeMessages(String topicName, String bootstrap, int numOfMessages, Duration timeout) {
        LOGGER.atInfo().log("Consuming messages using kcat");
        String name = Constants.KAFKA_CONSUMER_CLIENT_LABEL + "-kcat-" + TestUtils.getRandomPodNameSuffix();
        // Running consumer with parameters to get the latest N number of messages received to avoid consuming twice the same messages
        List<String> args = List.of("-b", bootstrap, "-K ,", "-t", topicName, "-C", "-o", "-" + numOfMessages, "-e", "-J");
        Job kCatClientJob = TestClientsJobTemplates.defaultKcatJob(name, args).build();
        String podName = KafkaUtils.createJob(deployNamespace, name, kCatClientJob);
        String log = waitForConsumer(deployNamespace, podName, timeout);
        LOGGER.atInfo().setMessage("Log: {}").addArgument(log).log();
        List<String> logRecords = extractRecordLinesFromLog(log);
        return getConsumerRecords(logRecords);
    }

    private String waitForConsumer(String namespace, String podName, Duration timeout) {
        DeploymentUtils.waitForPodRunSucceeded(namespace, podName, timeout);
        return kubeClient().logsInSpecificNamespace(namespace, podName);
    }

    private List<String> extractRecordLinesFromLog(String log) {
        return Stream.of(log.split("\n")).filter(TestUtils::isValidJson).toList();
    }

    private List<ConsumerRecord> getConsumerRecords(List<String> logRecords) {
        return logRecords.stream().map(x -> ConsumerRecord.parseFromJsonString(VALUE_TYPE_REF, x))
                .filter(Objects::nonNull).map(ConsumerRecord.class::cast).toList();
    }
}
