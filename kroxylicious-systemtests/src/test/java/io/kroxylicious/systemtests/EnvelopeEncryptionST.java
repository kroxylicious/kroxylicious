/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.Kafka;

import io.kroxylicious.systemtests.extensions.KroxyliciousExtension;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.vault.Vault;
import io.kroxylicious.systemtests.resources.vault.KubeVaultTestKmsFacade;
import io.kroxylicious.systemtests.resources.vault.KubeVaultTestKmsFacadeFactory;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KroxyliciousExtension.class)
class EnvelopeEncryptionST extends AbstractST {
    protected static final String BROKER_NODE_NAME = "kafka";
    private static final Logger LOGGER = LoggerFactory.getLogger(EnvelopeEncryptionST.class);
    private static final String MESSAGE = "Hello-world";
    private static KubeVaultTestKmsFacade kubeVaultTestKmsFacade;
    private final String clusterName = "my-cluster";
    private String bootstrap;

    @BeforeAll
    void setUp() {
        kubeVaultTestKmsFacade = new KubeVaultTestKmsFacadeFactory().build(Vault.VAULT_DEFAULT_NAMESPACE, Vault.VAULT_POD_NAME, cluster.isOpenshift());
        List<Pod> vaultPods = kubeClient().listPodsByPrefixInName(Vault.VAULT_DEFAULT_NAMESPACE, Vault.VAULT_SERVICE_NAME);
        if (!vaultPods.isEmpty()) {
            LOGGER.warn("Skipping vault deployment. It is already deployed!");
        }
        else {
            NamespaceUtils.createNamespaceWithWait(Vault.VAULT_DEFAULT_NAMESPACE);
            kubeVaultTestKmsFacade.start();
        }

        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.warn("Skipping kafka deployment. It is already deployed!");
            return;
        }
        LOGGER.info("Deploying Kafka in {} namespace", Constants.KAFKA_DEFAULT_NAMESPACE);

        Kafka kafka = KafkaTemplates.kafkaPersistentWithKRaftAnnotations(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, 3).build();

        resourceManager.createResourceWithWait(
                KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(BROKER_NODE_NAME, kafka, 3).build(),
                kafka);
    }

    @BeforeEach
    void beforeEach(String namespace) {
        var testKekManager = kubeVaultTestKmsFacade.getTestKekManager();
        testKekManager.generateKek(topicName);

        // start Kroxylicious
        LOGGER.info("Given Kroxylicious in {} namespace with {} replicas", namespace, 1);
        Kroxylicious kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlainWithRecordEncryptionFilter(clusterName, 1, topicName, kubeVaultTestKmsFacade.getKmsServiceConfig());
        bootstrap = kroxylicious.getBootstrap();

        LOGGER.info("And a kafka Topic named {}", topicName);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);
    }

    @AfterEach
    void afterEach(String namespace) {
        KafkaSteps.deleteTopic(namespace, topicName, bootstrap);
    }

    @AfterAll
    void cleanUp() {
        if (kubeVaultTestKmsFacade != null) {
            kubeVaultTestKmsFacade.stop();
        }
    }

    @Test
    void ensureClusterHasEncryptedMessage(String namespace) {
        int numberOfMessages = 1;
        String expectedMessage = MESSAGE + " - " + (numberOfMessages - 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String kafkaBootstrap = clusterName + "-kafka-bootstrap." + Constants.KAFKA_DEFAULT_NAMESPACE + ".svc.cluster.local:9092";
        String resultEncrypted = KroxyliciousSteps.consumeEncryptedMessages(namespace, topicName, kafkaBootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: {}", resultEncrypted);
        assertThat(resultEncrypted)
                .withFailMessage("expected message have not been received!")
                .contains(topicName + "vault");
    }

    @Test
    void produceAndConsumeMessage(String namespace) {
        int numberOfMessages = 1;
        String expectedMessage = MESSAGE + " - " + (numberOfMessages - 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: {}", result);
        assertThat(result)
                .withFailMessage("expected message have not been received!")
                .contains(expectedMessage);
    }
}
