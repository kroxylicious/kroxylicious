/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.Kafka;

import io.kroxylicious.systemtests.extensions.KroxyliciousExtension;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.vault.Vault;
import io.kroxylicious.systemtests.resources.vault.VaultTestKekManager;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(KroxyliciousExtension.class)
class TopicEncryptionST extends AbstractST {
    protected static final String BROKER_NODE_NAME = "kafka";
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicEncryptionST.class);
    private static final String MESSAGE = "Hello-world";
    private static Vault vaultOperator;
    private static Kroxylicious kroxylicious;
    private final String clusterName = "my-cluster";

    @BeforeAll
    void setUp() {
        List<Pod> vaultPods = kubeClient().listPodsByPrefixInName(Constants.VAULT_DEFAULT_NAMESPACE, Constants.VAULT_SERVICE_NAME);
        if (!vaultPods.isEmpty()) {
            LOGGER.warn("Skipping vault deployment. It is already deployed!");
        }
        else {
            vaultOperator = new Vault(Constants.VAULT_DEFAULT_NAMESPACE);
            NamespaceUtils.createNamespaceWithWait(Constants.VAULT_DEFAULT_NAMESPACE);
            vaultOperator.deploy();
        }

        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KROXY_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.warn("Skipping kafka deployment. It is already deployed!");
            return;
        }
        LOGGER.info("Deploying Kafka in {} namespace", Constants.KROXY_DEFAULT_NAMESPACE);

        Kafka kafka = KafkaTemplates.kafkaPersistentWithKRaftAnnotations(Constants.KROXY_DEFAULT_NAMESPACE, clusterName, 3).build();

        resourceManager.createResourceWithWait(
                KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(BROKER_NODE_NAME, kafka, 3).build(),
                kafka);
    }

    @AfterAll
    void cleanUp() throws IOException {
        if (vaultOperator != null) {
            vaultOperator.delete();
        }
    }

    @Test
    void produceAndConsumeEncryptedMessage(String namespace) {
        String topicName = "my-topic";
        int numberOfMessages = 1;
        String expectedMessage = MESSAGE + " - " + (numberOfMessages - 1);

        var testKekManager = new VaultTestKekManager(Constants.VAULT_DEFAULT_NAMESPACE, Constants.VAULT_SERVICE_NAME + "-0");
        testKekManager.generateKek(topicName);

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, 1);
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlainWithTopicEncryptionFilter(clusterName, 1, topicName);
        String bootstrap = kroxylicious.getBootstrap();

        LOGGER.info("And KafkaTopic in {} namespace", namespace);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String kafkaBootstrap = clusterName + "-kafka-bootstrap." + Constants.KROXY_DEFAULT_NAMESPACE + ".svc.cluster.local:9092";
        String resultEncrypted = KroxyliciousSteps.consumeEncryptedMessages(namespace, topicName, kafkaBootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: " + resultEncrypted);
        assertThat("'" + expectedMessage + "' expected message have not been received!", resultEncrypted.contains(topicName + "vault"));
    }

    @Test
    void produceAndConsumeMessage(String namespace) {
        String topicName = "my-topic2";
        int numberOfMessages = 1;
        String expectedMessage = MESSAGE + " - " + (numberOfMessages - 1);

        var testKekManager = new VaultTestKekManager(Constants.VAULT_DEFAULT_NAMESPACE, Constants.VAULT_SERVICE_NAME + "-0");
        testKekManager.generateKek(topicName);

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, 1);
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlainWithTopicEncryptionFilter(clusterName, 1, topicName);
        String bootstrap = kroxylicious.getBootstrap();

        LOGGER.info("And KafkaTopic in {} namespace", namespace);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: " + result);
        assertThat("'" + expectedMessage + "' expected message have not been received!", result.contains(expectedMessage));
    }
}
