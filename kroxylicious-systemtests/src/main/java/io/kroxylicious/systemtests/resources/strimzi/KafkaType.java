/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.strimzi;

import java.util.function.Consumer;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.skodjob.testframe.interfaces.ResourceType;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaList;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;

/**
 * The type Kafka resource.
 */
public class KafkaType implements ResourceType<Kafka> {

    @Override
    public MixedOperation<Kafka, KafkaList, io.fabric8.kubernetes.client.dsl.Resource<Kafka>> getClient() {
        return KubeClusterResource.kubeClient().getClient().resources(Kafka.class, KafkaList.class);
    }

    @Override
    public String getKind() {
        return Constants.STRIMZI_KAFKA_KIND;
    }

    @Override
    public void create(Kafka resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(Kafka resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void replace(Kafka resource, Consumer<Kafka> editor) {
        Kafka toBeUpdated = getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get();
        editor.accept(toBeUpdated);
        this.update(toBeUpdated);
    }

    @Override
    public boolean isReady(Kafka resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).isReady();
    }

    @Override
    public boolean isDeleted(Kafka resource) {
        return resource == null;
    }

    @Override
    public void update(Kafka resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }
}
