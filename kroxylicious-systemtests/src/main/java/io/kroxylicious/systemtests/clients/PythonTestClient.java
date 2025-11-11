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
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.skodjob.testframe.utils.KubeUtils;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.clients.records.PythonTestClientConsumerRecord;
import io.kroxylicious.systemtests.enums.KafkaClientType;
import io.kroxylicious.systemtests.templates.testclients.TestClientsJobTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.KafkaUtils;
import io.kroxylicious.systemtests.utils.TestUtils;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Python kafka test client (librdkafka client based CLI).
 */
public class PythonTestClient implements KafkaClient {
    private static final String RECEIVED_MESSAGE_MARKER = "Received:";
    private static final String PYTHON_COMMAND = "python3";
    private static final String BASE_PATH = "/usr/src";
    private static final String CONFLUENT_PYTHON_PATH = BASE_PATH + "/confluent-kafka-python";
    private static final String PRODUCER_PATH = CONFLUENT_PYTHON_PATH + "/Producer.py";
    private static final String CONSUMER_PATH = CONFLUENT_PYTHON_PATH + "/Consumer.py";
    private static final Logger LOGGER = LoggerFactory.getLogger(PythonTestClient.class);
    private static final TypeReference<PythonTestClientConsumerRecord> VALUE_TYPE_REF = new TypeReference<>() {
    };
    private String deployNamespace;

    @Override
    public KafkaClient inNamespace(String namespace) {
        this.deployNamespace = namespace;
        return this;
    }

    /**
     * Instantiates a new python test client.
     */
    public PythonTestClient() {
        this.deployNamespace = kubeClient().getNamespace();
    }

    @Override
    public void produceMessages(String topicName, String bootstrap, String message, @Nullable String messageKey, CompressionType compressionType, int numOfMessages) {
        final Optional<String> recordKey = Optional.ofNullable(messageKey);

        StringBuilder msg = new StringBuilder();
        for (int i = 0; i < numOfMessages; i++) {
            msg.append(message)
                    .append(" - ")
                    .append(i)
                    .append("\n");
        }

        LOGGER.atInfo().setMessage("Producing messages in '{}' topic using python").addArgument(topicName).log();
        String name = Constants.KAFKA_PRODUCER_CLIENT_LABEL + "-python-" + TestUtils.getRandomPodNameSuffix();
        String jsonOverrides = KubeUtils.isOcp() ? TestUtils.getJsonFileContent("nonJVMClient_openshift.json").replace("%NAME%", name) : "";

        List<String> executableCommand = new ArrayList<>(List.of(cmdKubeClient(deployNamespace).toString(), "run", "-i",
                "-n", deployNamespace, name,
                "--image=" + Constants.PYTHON_CLIENT_IMAGE,
                "--override-type=strategic",
                "--overrides=" + jsonOverrides,
                "--", PYTHON_COMMAND, PRODUCER_PATH, "-b", bootstrap, "-t", topicName, "-X", ProducerConfig.COMPRESSION_TYPE_CONFIG + "=" + compressionType.name));
        recordKey.ifPresent(key -> {
            executableCommand.add("-k");
            executableCommand.add(key);
        });

        KafkaUtils.produceMessagesWithCmd(deployNamespace, executableCommand, String.valueOf(msg), name, KafkaClientType.PYTHON_TEST_CLIENT.name().toLowerCase());
    }

    @Override
    public List<ConsumerRecord> consumeMessages(String topicName, String bootstrap, int numOfMessages, Duration timeout) {
        LOGGER.atInfo().log("Consuming messages using python");
        String name = Constants.KAFKA_CONSUMER_CLIENT_LABEL + "-python-" + TestUtils.getRandomPodNameSuffix();
        // Running consumer with parameters to get the latest N number of messages received to avoid consuming twice the same messages
        List<String> args = List.of(PYTHON_COMMAND, CONSUMER_PATH, "-n", String.valueOf(numOfMessages), "-b", bootstrap, "-t", topicName);
        Job pythonClientJob = TestClientsJobTemplates.defaultPythonJob(name, args).build();
        String podName = KafkaUtils.createJob(deployNamespace, name, pythonClientJob);
        String log = waitForConsumer(deployNamespace, podName, timeout);
        KafkaUtils.deleteJob(pythonClientJob);
        LOGGER.atInfo().setMessage("Pod STD_OUT: {}").addArgument(log).log();
        Stream<String> logRecords = extractRecordLinesFromLog(log);
        return getConsumerRecords(logRecords);
    }

    private String waitForConsumer(String namespace, String podName, Duration timeout) {
        DeploymentUtils.waitForPodRunSucceeded(namespace, podName, timeout);
        return kubeClient().logsInSpecificNamespace(namespace, podName);
    }

    private Stream<String> extractRecordLinesFromLog(String log) {
        return Stream.of(log.split("\n"))
                .filter(l -> l.startsWith(RECEIVED_MESSAGE_MARKER))
                .map(line -> line.substring(RECEIVED_MESSAGE_MARKER.length()));
    }

    private List<ConsumerRecord> getConsumerRecords(Stream<String> logRecords) {
        return logRecords.filter(Predicate.not(String::isBlank))
                .map(x -> ConsumerRecord.parseFromJsonString(VALUE_TYPE_REF, x))
                .filter(Objects::nonNull)
                .map(ConsumerRecord.class::cast)
                .toList();
    }
}
