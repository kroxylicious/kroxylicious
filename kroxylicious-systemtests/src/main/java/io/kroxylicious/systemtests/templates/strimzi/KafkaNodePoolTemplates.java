/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.strimzi;

import java.util.List;
import java.util.Map;

import io.strimzi.api.kafka.model.kafka.Kafka;
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
     * Kafka node pool with broker role.
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

    /**
     * Creates a KafkaNodePoolBuilder for a Kafka instance with both BROKER and CONTROLLER roles.
     *
     * @param nodePoolName The name of the node pool.
     * @param kafka The Kafka instance (Model of Kafka).
     * @param kafkaNodePoolReplicas The number of kafka broker replicas for the given node pool.
     * @return KafkaNodePoolBuilder configured with both BROKER and CONTROLLER roles.
     */
    public static KafkaNodePoolBuilder kafkaBasedNodePoolWithDualRole(String nodePoolName, Kafka kafka, int kafkaNodePoolReplicas) {
        return kafkaBasedNodePoolWithRole(nodePoolName, kafka, List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER), kafkaNodePoolReplicas);
    }

    private static KafkaNodePoolBuilder kafkaBasedNodePoolWithRole(String nodePoolName, Kafka kafka, List<ProcessRoles> roles, int kafkaNodePoolReplicas) {
        return new KafkaNodePoolBuilder()
                                         .withNewMetadata()
                                         .withName(nodePoolName)
                                         .withNamespace(kafka.getMetadata().getNamespace())
                                         .withLabels(Map.of(Constants.STRIMZI_CLUSTER_LABEL, kafka.getMetadata().getName()))
                                         .endMetadata()
                                         .withNewSpec()
                                         .withRoles(roles)
                                         .withReplicas(kafkaNodePoolReplicas)
                                         .withStorage(kafka.getSpec().getKafka().getStorage())
                                         .withJvmOptions(kafka.getSpec().getKafka().getJvmOptions())
                                         .withResources(kafka.getSpec().getKafka().getResources())
                                         .endSpec();
    }

    /**
     * Kafka node pool with controller role.
     *
     * @param namespaceName the namespace name
     * @param nodePoolName the node pool name
     * @param kafkaClusterName the kafka cluster name
     * @param kafkaReplicas the kafka replicas
     * @return the kafka node pool builder
     */
    public static KafkaNodePoolBuilder kafkaNodePoolWithControllerRole(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaNodePool(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas)
                                                                                                 .editOrNewSpec()
                                                                                                 .addToRoles(ProcessRoles.CONTROLLER)
                                                                                                 .endSpec();
    }

    /**
     * Kafka node pool with controller role and persistent storage.
     *
     * @param namespaceName the namespace name
     * @param nodePoolName the node pool name
     * @param kafkaClusterName the kafka cluster name
     * @param kafkaReplicas the kafka replicas
     * @return the kafka node pool builder
     */
    public static KafkaNodePoolBuilder kafkaNodePoolWithControllerRoleAndPersistentStorage(
            String namespaceName,
            String nodePoolName,
            String kafkaClusterName,
            int kafkaReplicas
    ) {
        return kafkaNodePoolWithControllerRole(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas)
                                                                                                            .editOrNewSpec()
                                                                                                            .withNewPersistentClaimStorage()
                                                                                                            .withSize("1Gi")
                                                                                                            .withDeleteClaim(true)
                                                                                                            .endPersistentClaimStorage()
                                                                                                            .endSpec();
    }

    /**
     * Kafka node pool with broker role and persistent storage.
     *
     * @param namespaceName the namespace name
     * @param nodePoolName the node pool name
     * @param kafkaClusterName the kafka cluster name
     * @param kafkaReplicas the kafka replicas
     * @return the kafka node pool builder
     */
    public static KafkaNodePoolBuilder kafkaNodePoolWithBrokerRoleAndPersistentStorage(
            String namespaceName,
            String nodePoolName,
            String kafkaClusterName,
            int kafkaReplicas
    ) {
        return kafkaNodePoolWithBrokerRole(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas)
                                                                                                        .editOrNewSpec()
                                                                                                        .withNewPersistentClaimStorage()
                                                                                                        .withSize("1Gi")
                                                                                                        .withDeleteClaim(true)
                                                                                                        .endPersistentClaimStorage()
                                                                                                        .endSpec();
    }
}
