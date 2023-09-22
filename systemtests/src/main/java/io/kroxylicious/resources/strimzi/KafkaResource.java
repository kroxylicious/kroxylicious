/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.resources.strimzi;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;

import io.kroxylicious.Constants;
import io.kroxylicious.enums.CustomResourceStatus;
import io.kroxylicious.resources.ResourceOperation;
import io.kroxylicious.resources.ResourceType;
import io.kroxylicious.resources.manager.ResourceManager;

import static io.kroxylicious.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kafka resource.
 */
public class KafkaResource implements ResourceType<Kafka> {

    @Override
    public String getKind() {
        return Constants.KAFKA_KIND;
    }

    @Override
    public Kafka get(String namespace, String name) {
        return kafkaClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(Kafka resource) {
        kafkaClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(Kafka resource) {
        kafkaClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(Kafka resource) {
        kafkaClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(Kafka resource) {
        return ResourceManager.waitForResourceStatus(kafkaClient(), resource, CustomResourceStatus.Ready,
                ResourceOperation.getTimeoutForResourceReadiness(Constants.KAFKA_KIND));
    }

    /**
     * Kafka client mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<Kafka, KafkaList, io.fabric8.kubernetes.client.dsl.Resource<Kafka>> kafkaClient() {
        return kubeClient().getClient().resources(Kafka.class, KafkaList.class);
    }
}
