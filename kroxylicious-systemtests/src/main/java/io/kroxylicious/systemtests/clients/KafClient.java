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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.batch.v1.Job;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.enums.KafkaClientType;
import io.kroxylicious.systemtests.templates.testclients.TestClientsJobTemplates;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import edu.umd.cs.findbugs.annotations.Nullable;

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
    public void produceMessages(String topicName, String bootstrap, String message, @Nullable String messageKey, int numOfMessages) {
        LOGGER.atInfo().setMessage("Producing messages in '{}' topic using kaf").addArgument(topicName).log();
        final Optional<String> recordKey = Optional.ofNullable(messageKey);
        String name = Constants.KAFKA_PRODUCER_CLIENT_LABEL + "-kaf";

        List<String> executableCommand = new ArrayList<>(List.of(cmdKubeClient(deployNamespace).toString(), "run", "-i",
                "-n", deployNamespace, name,
                "--image=" + Constants.KAF_CLIENT_IMAGE,
                "--", "kaf", "-n", String.valueOf(numOfMessages), "-b", bootstrap, "produce", topicName));
        recordKey.ifPresent(key -> {
            executableCommand.add("--key");
            executableCommand.add(key);
        });
        executableCommand.addAll(List.of("produce", topicName));

        KafkaUtils.produceMessagesWithCmd(deployNamespace, executableCommand, message, name, KafkaClientType.KAF.name().toLowerCase());
    }

    @Override
    public String consumeMessages(String topicName, String bootstrap, String messageToCheck, int numOfMessages, Duration timeout) {
        LOGGER.atInfo().log("Consuming messages using kaf");
        String name = Constants.KAFKA_CONSUMER_CLIENT_LABEL + "-kafka-go";
        List<String> args = List.of("kaf", "-b", bootstrap, "consume", topicName);
        Job goClientJob = TestClientsJobTemplates.defaultKafkaGoConsumerJob(name, args).build();
        return KafkaUtils.consumeMessages(topicName, name, deployNamespace, goClientJob, messageToCheck, numOfMessages, timeout);
    }
}
