/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.kroxylicious.systemtest.templates.crd;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.test.TestUtils;

import io.kroxylicious.systemtest.Constants;
import io.kroxylicious.systemtest.Labels;
import io.kroxylicious.systemtest.storage.TestStorage;

public class KafkaTopicTemplates {

    private KafkaTopicTemplates() {
    }

    public static KafkaTopicBuilder topic(TestStorage testStorage) {
        return defaultTopic(testStorage.getClusterName(), testStorage.getTopicName(), 1, 1, 1, testStorage.getNamespaceName());
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, String topicNamespace) {
        return defaultTopic(clusterName, topicName, 1, 1, 1, topicNamespace);
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions, String topicNamespace) {
        return defaultTopic(clusterName, topicName, partitions, 1, 1, topicNamespace);
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions, int replicas, String topicNamespace) {
        return defaultTopic(clusterName, topicName, partitions, replicas, replicas, topicNamespace);
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions, int replicas, int minIsr, String topicNamespace) {
        return defaultTopic(clusterName, topicName, partitions, replicas, minIsr, topicNamespace);
    }

    public static KafkaTopicBuilder defaultTopic(String clusterName, String topicName, int partitions, int replicas, int minIsr, String topicNamespace) {
        KafkaTopic kafkaTopic = getKafkaTopicFromYaml(Constants.PATH_TO_KAFKA_TOPIC_CONFIG);
        return new KafkaTopicBuilder(kafkaTopic)
                .withNewMetadata()
                .withName(topicName)
                .withNamespace(topicNamespace)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, clusterName)
                .endMetadata()
                .editSpec()
                .withPartitions(partitions)
                .withReplicas(replicas)
                .addToConfig("min.insync.replicas", minIsr)
                .endSpec();
    }

    private static KafkaTopic getKafkaTopicFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaTopic.class);
    }
}
