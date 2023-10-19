/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.strimzi;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceOperation;
import io.kroxylicious.systemtests.resources.ResourceType;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kafka topic resource.
 */
public class KafkaTopicResource implements ResourceType<KafkaTopic> {

    @Override
    public String getKind() {
        return Constants.KAFKA_TOPIC_KIND;
    }

    @Override
    public KafkaTopic get(String namespace, String name) {
        return kafkaTopicClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(KafkaTopic resource) {
        kafkaTopicClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaTopic resource) {
        kafkaTopicClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(KafkaTopic resource) {
        kafkaTopicClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(KafkaTopic resource) {
        return ResourceManager.waitForResourceStatusReady(kafkaTopicClient(), resource.getKind(), resource.getMetadata().getNamespace(),
                resource.getMetadata().getName(), ResourceOperation.getTimeoutForResourceReadiness(resource.getKind()));
    }

    /**
     * Kafka topic client mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<KafkaTopic, KafkaTopicList, io.fabric8.kubernetes.client.dsl.Resource<KafkaTopic>> kafkaTopicClient() {
        return kubeClient().getClient().resources(KafkaTopic.class, KafkaTopicList.class);
    }
}
