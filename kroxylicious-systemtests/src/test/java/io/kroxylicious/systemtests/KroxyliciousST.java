/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.common.record.CompressionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;

import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousBuilder;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaClusterRefTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyIngressTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousVirtualKafkaClusterTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;
import io.kroxylicious.systemtests.utils.KroxyliciousUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.BDDAssumptions.given;

/**
 * The Kroxylicious system tests.
 */
class KroxyliciousST extends AbstractSystemTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousST.class);
    private static Kroxylicious kroxylicious;
    private final String clusterName = "kroxylicious-st-cluster";
    private static final String MESSAGE = "Hello-world";
    private KroxyliciousOperator kroxyliciousOperator;

    private void deployPortIdentifiesNodeWithNoFilters(int replicas) {
        kroxylicious = new KroxyliciousBuilder()
                .withNamespace(Constants.KROXYLICIOUS_NAMESPACE)
                .withKafkaProxy(KroxyliciousKafkaProxyTemplates.defaultKafkaProxyCR(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, replicas).build())
                .withKafkaProxyIngress(KroxyliciousKafkaProxyIngressTemplates
                        .defaultKafkaProxyIngressCR(Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME).build())
                .withKafkaService(KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefCR(clusterName).build())
                .withVirtualKafkaCluster(KroxyliciousVirtualKafkaClusterTemplates.defaultVirtualKafkaClusterCR(clusterName,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP).build())
                .build();
        kroxylicious.createOrUpdateResources();
    }

    private void deployPortIdentifiesNodeWithNoFilters() {
        deployPortIdentifiesNodeWithNoFilters(1);
    }

    /**
     * Produce and consume message.
     *
     * @param namespace the namespace
     * @param compressionType the compression type
     */
    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void produceAndConsumeCompressedMessages(CompressionType compressionType, String namespace) {
        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(Constants.KROXYLICIOUS_NAMESPACE).addArgument(1).log();
        // Deploy kroxylicious operand only the first compression iteration (1 per testKmsFacade)
        if (compressionType.ordinal() == 0) {
            deployPortIdentifiesNodeWithNoFilters();
        }
        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        produceAndConsumeMessage(namespace, bootstrap, compressionType);
    }

    /**
     * Produce and consume message with TLS.
     *
     * @param namespace the namespace
     */
    @Test
    void produceAndConsumeMessagesWithTls(String namespace) {
        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        Tls tls = KroxyliciousUtils.createCertificateConfigMapFromListener(Constants.KROXYLICIOUS_NAMESPACE);
        kroxylicious = new KroxyliciousBuilder()
                .withNamespace(Constants.KROXYLICIOUS_NAMESPACE)
                .withTls(tls)
                .withKafkaProxy(KroxyliciousKafkaProxyTemplates.defaultKafkaProxyCR(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, 1).build())
                .withKafkaProxyIngress(KroxyliciousKafkaProxyIngressTemplates
                        .defaultKafkaProxyIngressCR(Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME).build())
                .withKafkaService(KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefCR(clusterName).build())
                .withVirtualKafkaCluster(KroxyliciousVirtualKafkaClusterTemplates.defaultVirtualKafkaClusterCR(clusterName,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP).build())
                .build();
        kroxylicious.createOrUpdateResources();
        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        produceAndConsumeMessage(namespace, bootstrap);
    }

    @Test
    void moreThanOneServicesInDifferentNamespaces(String namespace) {
        LOGGER.atInfo().setMessage("Given Kroxylicious service in {} namespace with {} replicas").addArgument(Constants.KROXYLICIOUS_NAMESPACE).addArgument(1).log();
        deployPortIdentifiesNodeWithNoFilters();

        String newNamespace = namespace + "-2";
        LOGGER.atInfo().setMessage("Given another Kroxylicious service in {} namespace with {} replicas").addArgument(newNamespace).addArgument(1).log();
        NamespaceUtils.createNamespaceAndPrepare(newNamespace);

        Kroxylicious kroxylicious2 = new KroxyliciousBuilder()
                .withNamespace(newNamespace)
                .withKafkaProxy(KroxyliciousKafkaProxyTemplates.defaultKafkaProxyCR(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, 1).build())
                .withKafkaProxyIngress(KroxyliciousKafkaProxyIngressTemplates
                        .defaultKafkaProxyIngressCR(Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME).build())
                .withKafkaService(KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefCR(clusterName).build())
                .withVirtualKafkaCluster(KroxyliciousVirtualKafkaClusterTemplates.defaultVirtualKafkaClusterCR(clusterName,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP).build())
                .build();
        kroxylicious2.createOrUpdateResources();

        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);
        assertThat(bootstrap).withFailMessage("bootstrap " + bootstrap + " does not contain the corresponding namespace " + Constants.KROXYLICIOUS_NAMESPACE)
                .contains(Constants.KROXYLICIOUS_NAMESPACE);
        produceAndConsumeMessage(namespace, bootstrap);

        bootstrap = kroxylicious2.getBootstrap(newNamespace, clusterName);
        assertThat(bootstrap).withFailMessage("bootstrap " + bootstrap + " does not contain the corresponding namespace " + newNamespace)
                .contains(newNamespace);
        produceAndConsumeMessage(newNamespace, bootstrap, randomTopicName(), CompressionType.NONE);
    }

    private void produceAndConsumeMessage(String namespace, String bootstrap) {
        produceAndConsumeMessage(namespace, bootstrap, topicName, CompressionType.NONE);
    }

    private void produceAndConsumeMessage(String namespace, String bootstrap, CompressionType compressionType) {
        produceAndConsumeMessage(namespace, bootstrap, topicName, compressionType);
    }

    private void produceAndConsumeMessage(String namespace, String bootstrap, String topicName, CompressionType compressionType) {
        int numberOfMessages = 1;

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1, compressionType);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, compressionType, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    /**
     * Restart kafka brokers.
     *
     * @param namespace the namespace
     */
    @Test
    void restartKafkaBrokers(String namespace) {
        int numberOfMessages = 25;

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(Constants.KROXYLICIOUS_NAMESPACE).addArgument(1).log();
        deployPortIdentifiesNodeWithNoFilters();
        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 1);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessagesWithoutWait(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        LOGGER.atInfo().setMessage("And a kafka broker is restarted").log();
        KafkaSteps.restartKafkaBroker(clusterName);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(5));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    /**
     * Kroxylicious with replicas.
     *
     * @param namespace the namespace
     */
    @Test
    void kroxyWithReplicas(String namespace) {
        int numberOfMessages = 3;
        int replicas = 3;

        // Clean up the kroxylicious instance to assure number of replicas
        cleanUpKroxyliciousInstance();

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(replicas).log();
        deployPortIdentifiesNodeWithNoFilters(replicas);
        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);
        int currentReplicas = kroxylicious.getNumberOfReplicas(Constants.KROXYLICIOUS_NAMESPACE);
        given(currentReplicas).withFailMessage("unexpected deployed replicas: " + currentReplicas).isEqualTo(replicas);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 1);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    @Test
    void scaleUpKroxylicious(String namespace) {
        scaleKroxylicious(namespace, 2, 3);
    }

    @Test
    void scaleDownKroxylicious(String namespace) {
        scaleKroxylicious(namespace, 3, 2);
    }

    private void scaleKroxylicious(String namespace, int replicas, int scaleTo) {
        int numberOfMessages = 10;

        // Clean up the kroxylicious instance to assure number of replicas
        cleanUpKroxyliciousInstance();

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(replicas).log();

        deployPortIdentifiesNodeWithNoFilters(replicas);
        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, clusterName);
        int currentReplicas = kroxylicious.getNumberOfReplicas(Constants.KROXYLICIOUS_NAMESPACE);
        given(currentReplicas).withFailMessage("unexpected deployed replicas: " + currentReplicas).isEqualTo(replicas);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 3, 1);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().setMessage("And kroxylicious is scaled to {}").addArgument(scaleTo).log();
        kroxylicious.scaleReplicasTo(Constants.KROXYLICIOUS_NAMESPACE, scaleTo, Duration.ofMinutes(2));
        currentReplicas = kroxylicious.getNumberOfReplicas(Constants.KROXYLICIOUS_NAMESPACE);
        assertThat(currentReplicas).withFailMessage("unexpected current scaled replicas").isEqualTo(scaleTo);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    @AfterAll
    void cleanUp() {
        if (kroxyliciousOperator != null) {
            kroxyliciousOperator.delete();
        }
    }

    /**
     * Sets before all.
     */
    @BeforeAll
    void setupBefore() {
        KafkaClients.getKafkaClient().preloadImage();
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
        }
        else {
            LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

            int kafkaReplicas = 1;
            resourceManager.createResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.poolWithDualRoleAndPersistentStorage(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas),
                    KafkaTemplates.defaultKafka(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas));
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    private void cleanUpKroxyliciousInstance() {
        NamespaceUtils.deleteNamespaceWithWait(Constants.KROXYLICIOUS_NAMESPACE);
        NamespaceUtils.createNamespaceAndPrepare(Constants.KROXYLICIOUS_NAMESPACE);
    }
}
