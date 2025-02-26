/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.resources.kms.ExperimentalKmsConfig;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousConfigMapTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousDeploymentTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousServiceTemplates;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Kroxylicious steps.
 */
public class KroxyliciousSteps {
    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousSteps.class);
    private static final String KROXY_URL = Environment.KROXY_IMAGE_REPO + (Environment.KROXY_IMAGE_REPO.endsWith(":") ? "" : ":");
    private static final String CONTAINER_IMAGE = KROXY_URL + Environment.KROXY_VERSION;
    private static final ResourceManager resourceManager = ResourceManager.getInstance();

    private KroxyliciousSteps() {
    }

    private static void createDefaultConfigMap(String clusterName, String deploymentNamespace) {
        LOGGER.info("Deploy Kroxylicious default config Map without filters in {} namespace", deploymentNamespace);
        resourceManager.createResourceWithWait(KroxyliciousConfigMapTemplates.defaultKroxyliciousConfig(clusterName, deploymentNamespace).build());
    }

    private static void createRecordEncryptionFilterConfigMap(String clusterName, String deploymentNamespace, TestKmsFacade<?, ?, ?> testKmsFacade,
                                                              ExperimentalKmsConfig experimentalKmsConfig) {
        LOGGER.info("Deploy Kroxylicious config Map with record encryption filter in {} namespace", deploymentNamespace);
        resourceManager
                .createResourceWithWait(
                        KroxyliciousConfigMapTemplates.kroxyliciousRecordEncryptionConfig(clusterName, deploymentNamespace, testKmsFacade, experimentalKmsConfig)
                                .build());
    }

    private static void deployPortPerBrokerPlain(String deploymentNamespace, int replicas) {
        LOGGER.info("Deploy Kroxylicious in {} namespace", deploymentNamespace);
        resourceManager.createResourceWithWait(KroxyliciousDeploymentTemplates.defaultKroxyDeployment(deploymentNamespace, CONTAINER_IMAGE, replicas).build());
        resourceManager.createResourceWithoutWait(KroxyliciousServiceTemplates.defaultKroxyService(deploymentNamespace).build());
    }

    /**
     * Deploy - Port per broker plain with no filters config
     * @param clusterName the cluster name
     * @param replicas the replicas
     */
    public static void deployPortPerBrokerPlainWithNoFilters(String clusterName, String namespace, int replicas) {
        createDefaultConfigMap(clusterName, namespace);
        deployPortPerBrokerPlain(namespace, replicas);
    }

    /**
     * Deploy port per broker plain with record encryption filter.
     *
     * @param clusterName the cluster name
     * @param replicas the replicas
     * @param testKmsFacade the test kms facade
     */
    public static void deployPortPerBrokerPlainWithRecordEncryptionFilter(String clusterName, String namespace, int replicas, TestKmsFacade<?, ?, ?> testKmsFacade) {
        deployPortPerBrokerPlainWithRecordEncryptionFilter(clusterName, namespace, replicas, testKmsFacade, null);
    }

    /**
     * Deploy port per broker plain with record encryption filter.
     *
     * @param clusterName the cluster name
     * @param replicas the replicas
     * @param testKmsFacade the test kms facade
     */
    public static void deployPortPerBrokerPlainWithRecordEncryptionFilter(String clusterName, String namespace, int replicas, TestKmsFacade<?, ?, ?> testKmsFacade,
                                                                          ExperimentalKmsConfig experimentalKmsConfig) {
        createRecordEncryptionFilterConfigMap(clusterName, namespace, testKmsFacade, experimentalKmsConfig);
        deployPortPerBrokerPlain(namespace, replicas);
    }

    /**
     * Scale replicas to.
     *
     * @param scaledTo the number of replicas to scale up/down
     * @param timeout the timeout
     */
    public static void scaleReplicasTo(String namespace, int scaledTo, Duration timeout) {
        LOGGER.info("Scaling number of replicas to {}..", scaledTo);
        kubeClient().getClient().apps().deployments().inNamespace(namespace).withName(Constants.KROXY_DEPLOYMENT_NAME).scale(scaledTo);
        await().atMost(timeout).pollInterval(Duration.ofSeconds(1))
                .until(() -> Kroxylicious.getNumberOfReplicas(namespace) == scaledTo && kubeClient().isDeploymentReady(namespace, Constants.KROXY_DEPLOYMENT_NAME));
    }

    /**
     * Produce messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numberOfMessages the number of messages
     */
    public static void produceMessages(String namespace, String topicName, String bootstrap, String message, int numberOfMessages) {
        KafkaClients.getKafkaClient().inNamespace(namespace).produceMessages(topicName, bootstrap, message, numberOfMessages);
    }

    /**
     * Consume messages.
     *
     * @param namespace the namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numberOfMessages the number of messages
     * @param timeout the timeout
     * @return the list of ConsumerRecords
     */
    public static List<ConsumerRecord> consumeMessages(String namespace, String topicName, String bootstrap, int numberOfMessages, Duration timeout) {
        return KafkaClients.getKafkaClient().inNamespace(namespace).consumeMessages(topicName, bootstrap, numberOfMessages, timeout);
    }

    /**
     * Kroxylicious will decrypt the message so to consume an encrypted message we need to read from the underlying Kafka cluster.
     *
     * @param clientNamespace where to run the client job
     * @param topicName the topic name
     * @param kafkaClusterName the name of the kafka cluster to read from
     * @param kafkaNamespace the namespace in which the broker is operating
     * @param numberOfMessages the number of messages
     * @param timeout maximum time to wait for the expectedMessage to appear
     * @return the list of consumer records
     */
    public static List<ConsumerRecord> consumeMessageFromKafkaCluster(String clientNamespace, String topicName, String kafkaClusterName,
                                                                      String kafkaNamespace, int numberOfMessages,
                                                                      Duration timeout) {
        String kafkaBootstrap = kafkaClusterName + "-kafka-bootstrap." + kafkaNamespace + ".svc.cluster.local:9092";
        return consumeMessages(clientNamespace, topicName, kafkaBootstrap, numberOfMessages, timeout);
    }
}
