/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

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
    private static final String ADMIN_CLIENT_TEMPLATE = "kafka-admin-client-template.yaml";
    private static final String DELIMITER = "\",\"";
    private static final String TOPIC_COMMAND = "topic";
    private static final String BOOTSTRAP_ARG = "--bootstrap-server=";
    private static final String NAME_VAR = "%NAME%";
    private static final String ARGS_VAR = "%ARGS%";
    private static final String KAFKA_VERSION_VAR = "%KAFKA_VERSION%";

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
        LOGGER.debug("Creating topic '{}' topic", topicName);
        String name = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-create";
        List<String> args = Arrays.asList(TOPIC_COMMAND, "create", BOOTSTRAP_ARG + bootstrap, "--topic=" + topicName, "--topic-partitions=" + partitions,
                "--topic-rep-factor=" + replicas);

        Job adminClientJob = TestClientsJobTemplates.defaultAdminClientJob(name, args).build();
        kubeClient().getClient().batch().v1().jobs().inNamespace(deployNamespace).resource(adminClientJob).create();
        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
        DeploymentUtils.waitForPodRunSucceeded(deployNamespace, podName, Duration.ofSeconds(30));
        String log = kubeClient().logsInSpecificNamespace(deployNamespace, podName);
        LOGGER.debug("Admin client create pod log: {}", log);
    }

    /**
     * Delete topic.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     */
    public static void deleteTopic(String deployNamespace, String topicName, String bootstrap) {
        if (!topicExists(deployNamespace, topicName, bootstrap)) {
            LOGGER.warn("Nothing to delete. Topic was not created");
            return;
        }
        LOGGER.debug("Deleting '{}' topic", topicName);
        String name = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-delete";
        List<String> args = Arrays.asList(TOPIC_COMMAND, "delete", BOOTSTRAP_ARG + bootstrap, "--topic=" + topicName);

        Job adminClientJob = TestClientsJobTemplates.defaultAdminClientJob(name, args).build();
        kubeClient().getClient().batch().v1().jobs().inNamespace(deployNamespace).resource(adminClientJob).create();

        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
        DeploymentUtils.waitForPodRunSucceeded(deployNamespace, podName, Duration.ofSeconds(30));
        String log = kubeClient().logsInSpecificNamespace(deployNamespace, podName);
        LOGGER.debug("Admin client delete pod log: {}", log);
    }

    private static boolean topicExists(String deployNamespace, String topicName, String bootstrap) {
        LOGGER.debug("List '{}' topic", topicName);
        String name = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-list";
        List<String> args = Arrays.asList(TOPIC_COMMAND, "list", BOOTSTRAP_ARG + bootstrap);

        Job adminClientJob = TestClientsJobTemplates.defaultAdminClientJob(name, args).build();
        kubeClient().getClient().batch().v1().jobs().inNamespace(deployNamespace).resource(adminClientJob).create();

        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
        DeploymentUtils.waitForPodRunSucceeded(deployNamespace, podName, Duration.ofSeconds(30));
        String log = kubeClient().logsInSpecificNamespace(deployNamespace, podName);
        LOGGER.debug("Admin client list pod log: {}", log);
        return log.contains(topicName);
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
