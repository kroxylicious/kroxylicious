/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.batch.v1.Job;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.templates.testclients.TestClientsJobTemplates;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

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
    }

    @Override
    public String consumeMessages(String topicName, String bootstrap, String messageToCheck, int numOfMessages, Duration timeout) {
        LOGGER.atInfo().log("Consuming messages using Strimzi Test Client");
        String name = Constants.KAFKA_CONSUMER_CLIENT_LABEL;
        Job testClientJob = TestClientsJobTemplates.defaultTestClientConsumerJob(name, bootstrap, topicName, numOfMessages).build();
        return KafkaUtils.consumeMessages(topicName, name, deployNamespace, testClientJob, messageToCheck, numOfMessages, timeout);
    }
}
