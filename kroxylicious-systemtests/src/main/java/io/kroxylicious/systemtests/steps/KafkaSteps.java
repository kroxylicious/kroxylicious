/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.kroxylicious.systemtests.Environment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static io.kroxylicious.systemtests.utils.KafkaUtils.getPodNameByLabel;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The type Kafka steps.
 */
public class KafkaSteps {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSteps.class);
    //private static final String ADMIN_CLIENT_COMMAND = "admin-client";
    private static final String ADMIN_CLIENT_TEMPLATE = "kafka-admin-client-template.yaml";
    private static final String TOPIC_COMMAND = "topic";
    private static final String BOOTSTRAP_ARG = "--bootstrap-server=";
    //private static final String NEVER_POLICY = "Never";
    private static final String NAMESPACE_VAR = "%NAMESPACE%";
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
        LOGGER.debug("Consuming messages from '{}' topic", topicName);
        String name = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-create";
        List<String> args = Arrays.asList(TOPIC_COMMAND, "create", BOOTSTRAP_ARG + bootstrap, "--topic=" + topicName, "--topic-partitions=" + partitions,
                "--topic-rep-factor=" + replicas);
        InputStream file = KafkaUtils.replaceStringInResourceFile(ADMIN_CLIENT_TEMPLATE, Map.of(
                NAMESPACE_VAR, deployNamespace,
                NAME_VAR, name,
                ARGS_VAR, String.join("\",\"", args),
                KAFKA_VERSION_VAR, Environment.KAFKA_VERSION));

        kubeClient().getClient().load(file).inNamespace(deployNamespace).create();
        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
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
        InputStream file = KafkaUtils.replaceStringInResourceFile(ADMIN_CLIENT_TEMPLATE, Map.of(
                NAMESPACE_VAR, deployNamespace,
                NAME_VAR, name,
                ARGS_VAR, String.join("\",\"", args),
                KAFKA_VERSION_VAR, Environment.KAFKA_VERSION));

        kubeClient().getClient().load(file).inNamespace(deployNamespace).create();
        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
        String log = kubeClient().logsInSpecificNamespace(deployNamespace, podName);
        LOGGER.debug("Admin client delete pod log: {}", log);
    }

//    public static void deleteTopic2(String deployNamespace, String topicName, String bootstrap) {
//        if (!topicExists(deployNamespace, topicName, bootstrap)) {
//            LOGGER.warn("Nothing to delete. Topic was not created");
//            return;
//        }
//        LOGGER.info("Deleting topic {}", topicName);
//        String podName = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-delete";
//        kubeClient().getClient().run().inNamespace(deployNamespace).withNewRunConfig()
//                .withImage(Constants.TEST_CLIENTS_IMAGE)
//                .withName(podName)
//                .withRestartPolicy(NEVER_POLICY)
//                .withCommand(ADMIN_CLIENT_COMMAND)
//                .withArgs(TOPIC_COMMAND, "delete", BOOTSTRAP_ARG + bootstrap, "--topic=" + topicName)
//                .done();
//
//        DeploymentUtils.waitForPodRunSucceeded(deployNamespace, podName, Duration.ofSeconds(30));
//        String log = kubeClient().logsInSpecificNamespace(deployNamespace, podName);
//        LOGGER.debug("Admin client delete pod log: {}", log);
//    }

    private static boolean topicExists(String deployNamespace, String topicName, String bootstrap) {
        LOGGER.debug("List '{}' topic", topicName);
        String name = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-list";
        List<String> args = Arrays.asList(TOPIC_COMMAND, "list", BOOTSTRAP_ARG + bootstrap);
        InputStream file = KafkaUtils.replaceStringInResourceFile(ADMIN_CLIENT_TEMPLATE, Map.of(
                NAMESPACE_VAR, deployNamespace,
                NAME_VAR, name,
                ARGS_VAR, String.join("\",\"", args),
                KAFKA_VERSION_VAR, Environment.KAFKA_VERSION));

        kubeClient().getClient().load(file).inNamespace(deployNamespace).create();
        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
        String log = kubeClient().logsInSpecificNamespace(deployNamespace, podName);
        LOGGER.debug("Admin client list pod log: {}", log);
        return log.contains(topicName);
    }

//    private static boolean topicExists2(String deployNamespace, String topicName, String bootstrap) {
//        LOGGER.info("Deleting topic {}", topicName);
//        String podName = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-list";
//        kubeClient().getClient().run().inNamespace(deployNamespace).withNewRunConfig()
//                .withImage(Constants.TEST_CLIENTS_IMAGE)
//                .withName(podName)
//                .withRestartPolicy(NEVER_POLICY)
//                .withCommand(ADMIN_CLIENT_COMMAND)
//                .withArgs(TOPIC_COMMAND, "list", BOOTSTRAP_ARG + bootstrap)
//                .done();
//
//        DeploymentUtils.waitForPodRunSucceeded(deployNamespace, podName, Duration.ofSeconds(30));
//        String log = kubeClient().logsInSpecificNamespace(deployNamespace, podName);
//        LOGGER.debug("Admin client list pod log: {}", log);
//        return log.contains(topicName);
//    }

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
