/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.strimzi;

import io.strimzi.api.kafka.model.common.template.ExternalTrafficPolicy;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationScramSha512Builder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.enums.LogLevel;

/**
 * The type Kafka templates.
 */
public class KafkaTemplates {

    private KafkaTemplates() {
    }

    /**
     * Kafka with external ip kafka builder.
     *
     * @param namespaceName the namespace name
     * @param clusterName the cluster name
     * @param kafkaReplicas the kafka replicas
     * @return the kafka builder
     */
    public static KafkaBuilder kafkaWithExternalIp(String namespaceName, String clusterName, int kafkaReplicas) {
        return defaultKafka(namespaceName, clusterName, kafkaReplicas)
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

    /**
     * Kafka with authentication kafka builder.
     *
     * @param namespaceName the namespace name
     * @param clusterName the cluster name
     * @param kafkaReplicas the kafka replicas
     * @return  the kafka builder
     */
    public static KafkaBuilder kafkaWithAuthentication(String namespaceName, String clusterName, int kafkaReplicas) {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpec();
        entityOperatorSpec.setUserOperator(new EntityUserOperatorSpec());
        return defaultKafka(namespaceName, clusterName, kafkaReplicas)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName(Constants.PLAIN_LISTENER_NAME)
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .withAuth(new KafkaListenerAuthenticationScramSha512Builder()
                                .build())
                        .build(),
                        new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .build())
                .endKafka()
                .withEntityOperator(entityOperatorSpec)
                .endSpec();
    }

    public static KafkaBuilder defaultKafka(String namespaceName, String clusterName, int kafkaReplicas) {
        // @formatter:off
        return new KafkaBuilder()
                .withApiVersion(Constants.KAFKA_API_VERSION_V1)
                .withKind(Constants.STRIMZI_KAFKA_KIND)
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespaceName)
                .endMetadata()
                .editSpec()
                    .editKafka()
                        .withVersion(Environment.KAFKA_VERSION)
                        .addToConfig("offsets.topic.replication.factor", Math.min(kafkaReplicas, 3))
                        .addToConfig("transaction.state.log.min.isr", Math.min(kafkaReplicas, 2))
                        .addToConfig("transaction.state.log.replication.factor", Math.min(kafkaReplicas, 3))
                        .addToConfig("default.replication.factor", Math.min(kafkaReplicas, 3))
                        .addToConfig("min.insync.replicas", Math.min(Math.max(kafkaReplicas - 1, 1), 2))
                        .addToConfig("auto.create.topics.enable", false)
                        .withListeners(
                                new GenericKafkaListenerBuilder()
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
                            .addToLoggers("rootLogger.level", LogLevel.INFO.name())
                        .endInlineLogging()
                    .endKafka()
                .endSpec();
        // @formatter:on
    }
}
