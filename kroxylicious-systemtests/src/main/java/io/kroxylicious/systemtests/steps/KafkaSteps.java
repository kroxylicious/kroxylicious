/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.batch.v1.Job;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousConfigMapTemplates;
import io.kroxylicious.systemtests.templates.testclients.TestClientsJobTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The type Kafka steps.
 */
public class KafkaSteps {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSteps.class);
    private static final String TOPIC_COMMAND = "topic";
    private static final String BOOTSTRAP_ARG = "--bootstrap-server=";
    private static final String TOPIC_ARG = "--topic=";

    private KafkaSteps() {
    }

    /**
     * Create topic.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param partitions the partitions
     * @param replicas the replicas
     */
    public static void createTopic(String deployNamespace, String topicName, String bootstrap, int partitions, int replicas) {
        createTopic(deployNamespace, topicName, bootstrap, partitions, replicas, CompressionType.NONE);
    }

    /**
     * Create topic.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param partitions the partitions
     * @param replicas the replicas
     * @param compressionType the compression type
     */
    public static void createTopic(String deployNamespace, String topicName, String bootstrap, int partitions, int replicas,
                                   @NonNull CompressionType compressionType) {
        LOGGER.atDebug().setMessage("Creating '{}' topic").addArgument(topicName).log();
        String name = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-create";
        List<String> args = new ArrayList<>(
                List.of(TOPIC_COMMAND, "create", BOOTSTRAP_ARG + bootstrap, TOPIC_ARG + topicName, "--topic-partitions=" + partitions,
                        "--topic-rep-factor=" + replicas));

        List<String> topicConfig = new ArrayList<>();
        if (!CompressionType.NONE.equals(compressionType)) {
            topicConfig.add(TopicConfig.COMPRESSION_TYPE_CONFIG + "=" + compressionType);
        }

        if (!topicConfig.isEmpty()) {
            args.add("--topic-config=" + String.join(",", topicConfig));
        }

        Job adminClientJob = TestClientsJobTemplates.defaultAdminClientJob(name, args).build();
        kubeClient().getClient().batch().v1().jobs().inNamespace(deployNamespace).resource(adminClientJob).create();
        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
        DeploymentUtils.waitForPodRunSucceeded(deployNamespace, podName, Duration.ofMinutes(1));
        LOGGER.atDebug().setMessage("Admin client create pod log: {}").addArgument(kubeClient().logsInSpecificNamespace(deployNamespace, podName)).log();
    }

    /**
     * Create topic with authentication.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param partitions the partitions
     * @param replicas the replicas
     * @param usernamePasswords the username passwords
     */
    public static void createTopicWithAuthentication(String deployNamespace, String topicName, String bootstrap, int partitions, int replicas,
                                                     Map<String, String> usernamePasswords) {
        if (!usernamePasswords.containsKey(Constants.KROXYLICIOUS_ADMIN_USER)) {
            throw new ConfigException("'admin' user not found! It is necessary to manage the topics");
        }

        LOGGER.atDebug().setMessage("Creating '{}' topic").addArgument(topicName).log();
        String name = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-create";
        List<String> args = new ArrayList<>(
                List.of(TOPIC_COMMAND, "create", BOOTSTRAP_ARG + bootstrap, TOPIC_ARG + topicName, "--topic-partitions=" + partitions,
                        "--topic-rep-factor=" + replicas));

        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                KroxyliciousConfigMapTemplates.getConfigMapForSaslConfig(deployNamespace, Constants.KAFKA_ADMIN_CLIENT_CONFIG_NAME, SecurityProtocol.SASL_PLAINTEXT.name,
                        ScramMechanism.SCRAM_SHA_512.mechanismName(), Constants.KROXYLICIOUS_ADMIN_USER, usernamePasswords.get(Constants.KROXYLICIOUS_ADMIN_USER)));

        Job adminClientJob = TestClientsJobTemplates.authenticationAdminClientJob(name, args).build();
        kubeClient().getClient().batch().v1().jobs().inNamespace(deployNamespace).resource(adminClientJob).create();
        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
        DeploymentUtils.waitForPodRunSucceeded(deployNamespace, podName, Duration.ofMinutes(5));
        LOGGER.atDebug().setMessage("Admin client create pod log: {}").addArgument(kubeClient().logsInSpecificNamespace(deployNamespace, podName)).log();
    }

    /**
     * Delete topic.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     */
    public static void deleteTopic(String deployNamespace, String topicName, String bootstrap) {
        LOGGER.atDebug().setMessage("Deleting '{}' topic").addArgument(topicName).log();
        String name = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-delete";
        List<String> args = List.of(TOPIC_COMMAND, "delete", BOOTSTRAP_ARG + bootstrap, "--if-exists", TOPIC_ARG + topicName);

        Job adminClientJob = TestClientsJobTemplates.defaultAdminClientJob(name, args).build();
        kubeClient().getClient().batch().v1().jobs().inNamespace(deployNamespace).resource(adminClientJob).create();

        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
        DeploymentUtils.waitForPodRunSucceeded(deployNamespace, podName, Duration.ofMinutes(1));
        LOGGER.atDebug().setMessage("Admin client delete pod log: {}").addArgument(kubeClient().logsInSpecificNamespace(deployNamespace, podName)).log();
    }

    /**
     * Restart kafka broker.
     *
     * @param clusterName the cluster name
     */
    public static void restartKafkaBroker(String clusterName) {
        clusterName = clusterName + "-kafka";
        assertThat("Broker has not been restarted successfully!", KafkaUtils.restartBroker(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName));
    }
}
