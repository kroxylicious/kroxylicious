/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.strimzi;

import java.util.List;
import java.util.Map;

import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;

import io.kroxylicious.systemtests.Constants;

/**
 * The type Kafka node pool templates.
 */
public class KafkaNodePoolTemplates {

    private static final String KAFKA_NODE_NAME_DUAL_ROLE = "kafka";
    private static final String KAFKA_NODE_NAME_BROKER_ROLE = "kafka-broker";
    private static final String KAFKA_NODE_NAME_CONTROLLER_ROLE = "kafka-controller";

    private KafkaNodePoolTemplates() {
    }

    /**
     * Creates a KafkaNodePool builder with specified roles and basic configuration.
     *
     * @param namespaceName the namespace name
     * @param kafkaClusterName the name of the Kafka cluster this pool belongs to
     * @param kafkaReplicas the number of replicas for this node pool
     * @param roles the process roles assigned to nodes (BROKER, CONTROLLER, or both)
     * @return a KafkaNodePoolBuilder with  storage defaulted to ephemeral
     */
    private static KafkaNodePoolBuilder defaultKafkaNodePool(String namespaceName, String kafkaClusterName, int kafkaReplicas, List<ProcessRoles> roles) {
        return new KafkaNodePoolBuilder()
                .editOrNewMetadata()
                .withNamespace(namespaceName)
                .withLabels(Map.of(Constants.STRIMZI_CLUSTER_LABEL, kafkaClusterName))
                .endMetadata()
                .withNewSpec()
                .withReplicas(kafkaReplicas)
                .withStorage(new EphemeralStorage())
                .withRoles(roles)
                .endSpec();
    }

    /**
     * Creates a KafkaNodePool builder configured with BROKER role and ephemeral storage.
     *
     * @param namespaceName the namespace name
     * @param kafkaClusterName the name of the Kafka cluster this pool belongs to
     * @param kafkaReplicas the number of broker replicas for this pool
     * @return a KafkaNodePoolBuilder with BROKER role and name "kafka-broker"
     */
    public static KafkaNodePoolBuilder poolWithBrokerRole(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaNodePool(namespaceName, kafkaClusterName, kafkaReplicas, List.of(ProcessRoles.BROKER))
                .editOrNewMetadata()
                .withName(KafkaNodePoolTemplates.KAFKA_NODE_NAME_BROKER_ROLE)
                .endMetadata();
    }

    /**
     * Creates a KafkaNodePool builder configured with CONTROLLER role and ephemeral storage.
     *
     * @param namespaceName the namespace name
     * @param kafkaClusterName the name of the Kafka cluster this pool belongs to
     * @param kafkaReplicas the number of broker (controller) replicas for this pool
     * @return a KafkaNodePoolBuilder with CONTROLLER role and name "kafka-controller"
     */
    public static KafkaNodePoolBuilder poolWithControllerRole(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaNodePool(namespaceName, kafkaClusterName, kafkaReplicas, List.of(ProcessRoles.CONTROLLER))
                .editOrNewMetadata()
                .withName(KafkaNodePoolTemplates.KAFKA_NODE_NAME_CONTROLLER_ROLE)
                .endMetadata();
    }

    /**
     * Creates a KafkaNodePoolBuilder for a Kafka instance with both BROKER and CONTROLLER roles and ephemeral storage.
     *
     * @param namespaceName the namespace name
     * @param kafkaClusterName the name of the Kafka cluster this pool belongs to
     * @param kafkaReplicas the number of replicas for the given node pool
     * @return KafkaNodePoolBuilder configured with both BROKER and CONTROLLER roles
     */
    public static KafkaNodePoolBuilder poolWithDualRole(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaNodePool(namespaceName, kafkaClusterName, kafkaReplicas, List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER))
                .editOrNewMetadata()
                .withName(KafkaNodePoolTemplates.KAFKA_NODE_NAME_DUAL_ROLE)
                .endMetadata();
    }

    /**
     * Creates a KafkaNodePool builder configured with BROKER role and persistent storage.
     *
     * @param namespaceName the namespace name
     * @param kafkaClusterName the name of the Kafka cluster this pool belongs to
     * @param kafkaReplicas the number of broker replicas for this pool
     * @return a KafkaNodePoolBuilder with BROKER role, name "kafka-broker" and persistent storage
     */
    public static KafkaNodePoolBuilder poolWithBrokerRoleAndPersistentStorage(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return poolWithBrokerRole(namespaceName, kafkaClusterName, kafkaReplicas)
                .editOrNewSpec()
                .withNewPersistentClaimStorage()
                .withSize("1Gi")
                .withDeleteClaim(true)
                .endPersistentClaimStorage()
                .endSpec();
    }

    /**
     * Creates a KafkaNodePool builder configured with CONTROLLER role and persistent storage.
     *
     * @param namespaceName the namespace name
     * @param kafkaClusterName the name of the Kafka cluster this pool belongs to
     * @param kafkaReplicas the number of kafka (controller) replicas for this pool
     * @return a KafkaNodePoolBuilder with CONTROLLER role, name "kafka-controller" and persistent storage
     */
    public static KafkaNodePoolBuilder poolWithControllerRoleAndPersistentStorage(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return poolWithControllerRole(namespaceName, kafkaClusterName, kafkaReplicas)
                .editOrNewSpec()
                .withNewPersistentClaimStorage()
                .withSize("1Gi")
                .withDeleteClaim(true)
                .endPersistentClaimStorage()
                .endSpec();
    }

    /**
     * Creates a KafkaNodePool builder configured with both roles (BROKER, CONTROLLER) and persistent storage.
     *
     * @param namespaceName the namespace name
     * @param kafkaClusterName the name of the Kafka cluster this pool belongs to
     * @param kafkaReplicas the number of kafka (broker, controller) replicas for this pool
     * @return a KafkaNodePoolBuilder with both roles, name "kafka" and persistent storage
     */
    public static KafkaNodePoolBuilder poolWithDualRoleAndPersistentStorage(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return poolWithDualRole(namespaceName, kafkaClusterName, kafkaReplicas)
                .editOrNewSpec()
                .withNewPersistentClaimStorage()
                .withSize("1Gi")
                .withDeleteClaim(true)
                .endPersistentClaimStorage()
                .endSpec();
    }
}
