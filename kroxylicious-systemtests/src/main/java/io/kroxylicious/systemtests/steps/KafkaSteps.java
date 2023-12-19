/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.io.ByteArrayOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTopicTemplates;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The type Kafka steps.
 */
public class KafkaSteps {
    private static final ResourceManager resourceManager = ResourceManager.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSteps.class);

    private KafkaSteps() {
    }

    /**
     * Create topic.
     *
     * @param clusterName the cluster name
     * @param topicName the topic name
     * @param namespace the namespace
     * @param partitions the partitions
     * @param replicas the replicas
     * @param minIsr the min isr
     */
    public static void createTopic(String clusterName, String topicName, String namespace,
                                   int partitions, int replicas, int minIsr) {
        resourceManager.createResourceWithWait(
                KafkaTopicTemplates.defaultTopic(namespace, clusterName, topicName, partitions, replicas, minIsr).build());
    }

    /**
     * Create topic using test clients.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param partitions the partitions
     * @param replicas the replicas
     */
    public static void createTopicTestClient(String deployNamespace, String topicName, String bootstrap, int partitions, int replicas) {
        String podName = KafkaUtils.adminTestClient(deployNamespace, bootstrap);
        String command = "admin-client topic create --bootstrap-server=" + bootstrap + " --topic=" + topicName + " --topic-partitions=" + partitions +
                " --topic-rep-factor=" + replicas;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        var exitCode = kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName)
                .writingOutput(baos)
                .writingError(baos)
                .exec("sh", "-c", command)
                .exitCode().join();

        if (exitCode != 0) {
            LOGGER.error(baos.toString());
            throw new KubeClusterException(new ExecResult(exitCode, baos.toString(), baos.toString()), "Topic creation failed! Exit code: " + exitCode);
        }
        else {
            LOGGER.debug(baos.toString());
        }
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
