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
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param partitions the partitions
     * @param replicas the replicas
     */
    public static void createTopic(String deployNamespace, String topicName, String bootstrap, int partitions, int replicas) {
        String command = "admin-client topic create --bootstrap-server=" + bootstrap + " --topic=" + topicName + " --topic-partitions=" + partitions +
                " --topic-rep-factor=" + replicas;

        kubeClient().getClient().run().inNamespace(deployNamespace).withNewRunConfig()
                .withImage(Constants.TEST_CLIENTS_IMAGE)
                .withName(Constants.KAFKA_ADMIN_CLIENT_LABEL)
                .withRestartPolicy("Never")
                .withCommand("/bin/sh")
                .withArgs("-c", command)
                .done();

        DeploymentUtils.waitForRunSucceeded(deployNamespace, Constants.KAFKA_ADMIN_CLIENT_LABEL, Duration.ofSeconds(10));
        LOGGER.debug("Admin client pod log: {}", kubeClient().logsInSpecificNamespace(deployNamespace, Constants.KAFKA_ADMIN_CLIENT_LABEL));
    }

    /**
     * Restart kafka broker.
     *
     * @param clusterName the cluster name
     */
    public static void restartKafkaBroker(String clusterName) {
        clusterName = clusterName + "-kafka";
        assertThat("Broker has not been restarted successfully!", KafkaUtils.restartBroker(Constants.KROXY_DEFAULT_NAMESPACE, clusterName));
    }
}
