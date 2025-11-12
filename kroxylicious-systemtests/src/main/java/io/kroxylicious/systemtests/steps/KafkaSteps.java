/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.batch.v1.Job;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.templates.testclients.TestClientsJobTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The type Kafka steps.
 */
public class KafkaSteps {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSteps.class);
    private static final String TOPIC_COMMAND = "topic";
    private static final String BOOTSTRAP_ARG = "--bootstrap-server=";

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
    public static void createTopic(String deployNamespace, String topicName, String bootstrap, int partitions, int replicas, CompressionType compressionType) {
        LOGGER.atDebug().setMessage("Creating '{}' topic").addArgument(topicName).log();
        String name = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-create";
        List<String> args = new ArrayList<>(
                List.of(TOPIC_COMMAND, "create", BOOTSTRAP_ARG + bootstrap, "--topic=" + topicName, "--topic-partitions=" + partitions,
                        "--topic-rep-factor=" + replicas));

        if (!CompressionType.NONE.equals(compressionType)) {
            args.add("--topic-config=" + TopicConfig.COMPRESSION_TYPE_CONFIG + "=" + compressionType);
        }

        Job adminClientJob = TestClientsJobTemplates.defaultAdminClientJob(name, args).build();
        kubeClient().getClient().batch().v1().jobs().inNamespace(deployNamespace).resource(adminClientJob).create();
        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
        DeploymentUtils.waitForPodRunSucceeded(deployNamespace, podName, Duration.ofMinutes(1));
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
        List<String> args = List.of(TOPIC_COMMAND, "delete", BOOTSTRAP_ARG + bootstrap, "--if-exists", "--topic=" + topicName);

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
