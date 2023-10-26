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

import static org.junit.Assert.assertTrue;

/**
 * The type Kafka steps.
 */
public class KafkaSteps {
    private static final ResourceManager resourceManager = ResourceManager.getInstance();

    /**
     * Create topic.
     *
     * @param displayName the display name
     * @param clusterName the cluster name
     * @param topicName the topic name
     * @param namespace the namespace
     * @param partitions the partitions
     * @param replicas the replicas
     * @param minIsr the min isr
     */
    public static void createTopic(String displayName, String clusterName, String topicName, String namespace,
                                   int partitions, int replicas, int minIsr) {
        resourceManager.createResourceWithWait(displayName,
                KafkaTopicTemplates.defaultTopic(namespace, clusterName, topicName, partitions, replicas, minIsr).build());
    }

    /**
     * Restart kakfa broker.
     *
     * @param clusterName the cluster name
     */
    public static void restartKakfaBroker(String clusterName) {
        clusterName = clusterName + "-kafka";
        assertTrue(KafkaUtils.restartBroker(Constants.KROXY_DEFAULT_NAMESPACE, clusterName));
    }
}
