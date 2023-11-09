/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.strimzi;

import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.template.ExternalTrafficPolicy;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.enums.LogLevel;
import io.kroxylicious.systemtests.utils.KafkaVersionUtils;

/**
 * The type Kafka templates.
 */
public class KafkaTemplates {

    /**
     * Kafka persistent kafka builder.
     *
     * @param namespaceName the namespace name
     * @param clusterName the cluster name
     * @param kafkaReplicas the kafka replicas
     * @param zkReplicas the zk replicas
     * @return the kafka builder
     */
    public static KafkaBuilder kafkaPersistent(String namespaceName, String clusterName, int kafkaReplicas, int zkReplicas) {
        return defaultKafka(namespaceName, clusterName, kafkaReplicas, zkReplicas)
                .editSpec()
                .editKafka()
                .withNewPersistentClaimStorage()
                .withSize("1Gi")
                .withDeleteClaim(true)
                .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                .withNewPersistentClaimStorage()
                .withSize("1Gi")
                .withDeleteClaim(true)
                .endPersistentClaimStorage()
                .endZookeeper()
                .endSpec();
    }

    /**
     * Kafka persistent with external ip kafka builder.
     *
     * @param namespaceName the namespace name
     * @param clusterName the cluster name
     * @param kafkaReplicas the kafka replicas
     * @param zkReplicas the zk replicas
     * @return the kafka builder
     */
    public static KafkaBuilder kafkaPersistentWithExternalIp(String namespaceName, String clusterName, int kafkaReplicas, int zkReplicas) {
        return kafkaPersistent(namespaceName, clusterName, kafkaReplicas, zkReplicas)
                .editSpec()
                .editKafka()
                .addToListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withTls(false)
                        .withPort(9094)
                        .withType(KafkaListenerType.LOADBALANCER)
                        .editOrNewConfiguration()
                        .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                        .endConfiguration()
                        .build())
                .endKafka()
                .endSpec();
    }

    private static KafkaBuilder defaultKafka(String namespaceName, String clusterName, int kafkaReplicas, int zkReplicas) {
        return new KafkaBuilder()
                .withApiVersion(Constants.KAFKA_API_VERSION_V1BETA2)
                .withKind(Constants.KAFKA_KIND)
                .withNewMetadata()
                .withName(clusterName)
                .withNamespace(namespaceName)
                .endMetadata()
                .editSpec()
                .editKafka()
                .withVersion(Environment.KAFKA_VERSION)
                .withReplicas(kafkaReplicas)
                .addToConfig("log.message.format.version", KafkaVersionUtils.getKafkaProtocolVersion(Environment.KAFKA_VERSION))
                .addToConfig("inter.broker.protocol.version", KafkaVersionUtils.getKafkaProtocolVersion(Environment.KAFKA_VERSION))
                .addToConfig("offsets.topic.replication.factor", Math.min(kafkaReplicas, 3))
                .addToConfig("transaction.state.log.min.isr", Math.min(kafkaReplicas, 2))
                .addToConfig("transaction.state.log.replication.factor", Math.min(kafkaReplicas, 3))
                .addToConfig("default.replication.factor", Math.min(kafkaReplicas, 3))
                .addToConfig("min.insync.replicas", Math.min(Math.max(kafkaReplicas - 1, 1), 2))
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName(Constants.PLAIN_LISTENER_NAME)
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .build(),
                        new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .build())
                .withNewInlineLogging()
                .addToLoggers("kafka.root.logger.level", LogLevel.INFO.name())
                .endInlineLogging()
                .endKafka()
                .editZookeeper()
                .withReplicas(zkReplicas)
                .withNewInlineLogging()
                .addToLoggers("zookeeper.root.logger", LogLevel.INFO.name())
                .endInlineLogging()
                .endZookeeper()
                .editEntityOperator()
                .editUserOperator()
                .withNewInlineLogging()
                .addToLoggers("rootLogger.level", LogLevel.INFO.name())
                .endInlineLogging()
                .endUserOperator()
                .editTopicOperator()
                .withNewInlineLogging()
                .addToLoggers("rootLogger.level", LogLevel.INFO.name())
                .endInlineLogging()
                .endTopicOperator()
                .endEntityOperator()
                .endSpec();
    }
}
