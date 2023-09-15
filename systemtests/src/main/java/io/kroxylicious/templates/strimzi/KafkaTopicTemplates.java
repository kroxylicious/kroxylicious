/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.templates.strimzi;

import io.strimzi.api.kafka.model.KafkaTopicBuilder;

import io.kroxylicious.Constants;

public class KafkaTopicTemplates {

    public static KafkaTopicBuilder defaultTopic(String topicNamespace, String clusterName, String topicName, int partitions, int replicas, int minIsr) {
        return new KafkaTopicBuilder()
                .withApiVersion(Constants.KAFKA_API_VERSION_V1BETA2)
                .withKind(Constants.KAFKA_TOPIC_KIND)
                .withNewMetadata()
                .withName(topicName)
                .withNamespace(topicNamespace)
                .addToLabels(Constants.STRIMZI_CLUSTER_LABEL, clusterName)
                .endMetadata()
                .editSpec()
                .withPartitions(partitions)
                .withReplicas(replicas)
                .addToConfig("min.insync.replicas", minIsr)
                .endSpec();
    }
}
