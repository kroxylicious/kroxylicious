/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The type Kafka steps.
 */
public class KafkaSteps {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSteps.class);

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
        String podName = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-create";
        kubeClient().getClient().run().inNamespace(deployNamespace).withNewRunConfig()
                .withImage(Constants.TEST_CLIENTS_IMAGE)
                .withName(podName)
                .withRestartPolicy("Never")
                .withCommand("admin-client")
                .withArgs("topic", "create", "--bootstrap-server=" + bootstrap, "--topic=" + topicName, "--topic-partitions=" + partitions,
                        "--topic-rep-factor=" + replicas)
                .done();

        DeploymentUtils.waitForPodRunSucceeded(deployNamespace, podName, Duration.ofSeconds(30));
        LOGGER.debug("Admin client pod log: {}", kubeClient().logsInSpecificNamespace(deployNamespace, podName));
    }

    /**
     * Delete topic.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     */
    public static void deleteTopic(String deployNamespace, String topicName, String bootstrap) {
        LOGGER.info("Deleting topic {}", topicName);
        String podName = Constants.KAFKA_ADMIN_CLIENT_LABEL + "-delete";
        kubeClient().getClient().run().inNamespace(deployNamespace).withNewRunConfig()
                .withImage(Constants.TEST_CLIENTS_IMAGE)
                .withName(podName)
                .withRestartPolicy("Never")
                .withCommand("admin-client")
                .withArgs("topic", "delete", "--bootstrap-server=" + bootstrap, "--topic=" + topicName)
                .done();

        DeploymentUtils.waitForPodRunSucceeded(deployNamespace, podName, Duration.ofSeconds(30));
        LOGGER.debug("Admin client pod log: {}", kubeClient().logsInSpecificNamespace(deployNamespace, podName));
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
