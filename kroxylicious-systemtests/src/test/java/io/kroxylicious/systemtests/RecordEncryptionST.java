/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.Kafka;

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
class RecordEncryptionST extends AbstractST {
    protected static final String BROKER_NODE_NAME = "kafka";
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordEncryptionST.class);
    private static final String MESSAGE = "Hello-world";
    private static KubeVaultTestKmsFacade kubeVaultTestKmsFacade;
    private final String clusterName = "my-cluster";
    private String bootstrap;

    @BeforeAll
    void setUp() {
        kubeVaultTestKmsFacade = new KubeVaultTestKmsFacadeFactory().build(Vault.VAULT_DEFAULT_NAMESPACE, Vault.VAULT_POD_NAME, cluster.isOpenshift());
        List<Pod> vaultPods = kubeClient().listPodsByPrefixInName(Vault.VAULT_DEFAULT_NAMESPACE, Vault.VAULT_SERVICE_NAME);
        if (!vaultPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping vault deployment. It is already deployed!").log();
        }
        else {
            NamespaceUtils.createNamespaceWithWait(Vault.VAULT_DEFAULT_NAMESPACE);
            kubeVaultTestKmsFacade.start();
        }

        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
            return;
        }
        LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

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
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        Kroxylicious kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlainWithRecordEncryptionFilter(clusterName, 1, topicName, kubeVaultTestKmsFacade.getKmsServiceConfig());
        bootstrap = kroxylicious.getBootstrap();

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
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

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        String resultEncrypted = KroxyliciousSteps.consumeMessageFromKafkaCluster(namespace, topicName, clusterName, Constants.KAFKA_DEFAULT_NAMESPACE, numberOfMessages,
                Duration.ofMinutes(2), "key: kroxylicious.io/encryption");
        LOGGER.atInfo().setMessage("Received: {}").addArgument(resultEncrypted).log();
        assertThat(resultEncrypted)
                .withFailMessage("expected message have not been received!")
                .contains(topicName + "vault");
    }

    @Test
    void produceAndConsumeMessage(String namespace) {
        int numberOfMessages = 1;

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        String result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();
        assertThat(StringUtils.countMatches(result, MESSAGE)).withFailMessage("expected messages have not been received!").isEqualTo(numberOfMessages);
    }
}
