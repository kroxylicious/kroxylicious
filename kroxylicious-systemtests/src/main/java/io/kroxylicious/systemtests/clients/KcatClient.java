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
    public void produceMessages(String topicName, String bootstrap, String message, int numOfMessages) {
        StringBuilder msg = new StringBuilder();
        for (int i = 0; i < numOfMessages; i++) {
            msg.append(message + " - " + i + "\n");
        }

        LOGGER.atInfo().setMessage("Producing messages in '{}' topic using kcat").addArgument(topicName).log();
        String name = Constants.KAFKA_PRODUCER_CLIENT_LABEL + "-kcat";
        List<String> executableCommand = Arrays.asList(cmdKubeClient(deployNamespace).toString(), "run", "-i",
                "-n", deployNamespace, name,
                "--image=" + Constants.KCAT_CLIENT_IMAGE,
                "--", "-b", bootstrap, "-l", "-t", topicName, "-P");

        LOGGER.atInfo().setMessage("Executing command: {} for running kcat producer").addArgument(executableCommand).log();
        ExecResult result = Exec.exec(String.valueOf(msg), executableCommand, Duration.ofSeconds(30), true, false, null);

        String log = kubeClient().logsInSpecificNamespace(deployNamespace, name);
        if (result.isSuccess()) {
            LOGGER.atInfo().setMessage("kcat client produce log: {}").addArgument(log).log();
        }
        else {
            LOGGER.atError().setMessage("error producing messages with kcat: {}").addArgument(log).log();
            throw new KubernetesClientException("error producing messages with kcat: " + log);
        }
    }

    @Override
    public String consumeMessages(String topicName, String bootstrap, String messageToCheck, int numOfMessages, Duration timeout) {
        String name = Constants.KAFKA_CONSUMER_CLIENT_LABEL + "-kcat";
        List<String> args = Arrays.asList("-b", bootstrap, "-t", topicName, "-C", "-c" + numOfMessages);
        Job kCatClientJob = TestClientsJobTemplates.defaultKcatJob(name, args).build();
        return KafkaUtils.consumeMessages(topicName, name, deployNamespace, kCatClientJob, messageToCheck, timeout);
    }
}
