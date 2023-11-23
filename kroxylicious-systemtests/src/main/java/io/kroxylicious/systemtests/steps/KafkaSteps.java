/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTopicTemplates;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The type Kafka steps.
 */
public class KafkaSteps {
    private static final ResourceManager resourceManager = ResourceManager.getInstance();

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
     * Restart kafka broker.
     *
     * @param clusterName the cluster name
     */
    public static void restartKafkaBroker(String clusterName) {
        clusterName = clusterName + "-kafka";
        assertThat("Broker has not been restarted successfully!", KafkaUtils.restartBroker(Constants.KROXY_DEFAULT_NAMESPACE, clusterName));
    }
}
