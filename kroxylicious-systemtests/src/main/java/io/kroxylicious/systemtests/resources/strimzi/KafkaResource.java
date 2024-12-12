/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.strimzi;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaList;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceManager;
import io.kroxylicious.systemtests.resources.ResourceOperation;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

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
        return ResourceManager.waitForResourceStatusReady(kafkaClient(), resource,
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
