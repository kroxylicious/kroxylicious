/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClientException;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.templates.testclients.TestClientsJobTemplates;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kaf client (sarama client based CLI).
 */
public class KafClient implements KafkaClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafClient.class);
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
    public void produceMessages(String topicName, String bootstrap, String message, int numOfMessages) {
        LOGGER.atInfo().setMessage("Producing messages in '{}' topic using kaf").addArgument(topicName).log();
        String name = Constants.KAFKA_PRODUCER_CLIENT_LABEL + "-kaf";
        List<String> executableCommand = Arrays.asList(cmdKubeClient(deployNamespace).toString(), "run", "-i",
                "-n", deployNamespace, name,
                "--image=" + Constants.KAF_CLIENT_IMAGE,
                "--", "kaf", "-n", String.valueOf(numOfMessages), "-b", bootstrap, "produce", topicName);

        LOGGER.atInfo().setMessage("Executing command: {} for running kaf producer").addArgument(executableCommand).log();
        ExecResult result = Exec.exec(message, executableCommand, Duration.ofSeconds(30), true, false, null);

        String log = kubeClient().logsInSpecificNamespace(deployNamespace, name);
        if (result.isSuccess()) {
            LOGGER.atInfo().setMessage("kaf client produce log: {}").addArgument(log).log();
        }
        else {
            LOGGER.atError().setMessage("error producing messages with kaf: {}").addArgument(log).log();
            throw new KubernetesClientException("error producing messages with kaf: " + log);
        }
    }

    @Override
    public String consumeMessages(String topicName, String bootstrap, String messageToCheck, int numOfMessages, Duration timeout) {
        String name = Constants.KAFKA_CONSUMER_CLIENT_LABEL + "-kafka-go";
        List<String> args = Arrays.asList("kaf", "-b", bootstrap, "consume", topicName);
        Job goClientJob = TestClientsJobTemplates.defaultKafkaGoConsumerJob(name, args).build();
        return KafkaUtils.consumeMessages(topicName, name, deployNamespace, goClientJob, messageToCheck, timeout);
    }
}
