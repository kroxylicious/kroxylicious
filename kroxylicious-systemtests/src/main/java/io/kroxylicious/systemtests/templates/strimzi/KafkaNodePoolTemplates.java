/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.strimzi;

import java.util.Map;

import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;

import io.kroxylicious.systemtests.Constants;

/**
 * The type Kafka node pool templates.
 */
public class KafkaNodePoolTemplates {

    /**
     * Default kafka node pool kafka node pool builder.
     *
     * @param namespaceName the namespace name
     * @param nodePoolName the node pool name
     * @param kafkaClusterName the kafka cluster name
     * @param kafkaReplicas the kafka replicas
     * @return the kafka node pool builder
     */
    public static KafkaNodePoolBuilder defaultKafkaNodePool(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return new KafkaNodePoolBuilder()
                .withNewMetadata()
                .withNamespace(namespaceName)
                .withName(nodePoolName)
                .withLabels(Map.of(Constants.STRIMZI_CLUSTER_LABEL, kafkaClusterName))
                .endMetadata()
                .withNewSpec()
                .withReplicas(kafkaReplicas)
                .endSpec();
    }

    /**
     * Kafka node pool with broker role kafka node pool builder.
     *
     * @param namespaceName the namespace name
     * @param nodePoolName the node pool name
     * @param kafkaClusterName the kafka cluster name
     * @param kafkaReplicas the kafka replicas
     * @return the kafka node pool builder
     */
    public static KafkaNodePoolBuilder kafkaNodePoolWithBrokerRole(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaNodePool(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas)
                .editOrNewSpec()
                .addToRoles(ProcessRoles.BROKER)
                .endSpec();
    }
}
