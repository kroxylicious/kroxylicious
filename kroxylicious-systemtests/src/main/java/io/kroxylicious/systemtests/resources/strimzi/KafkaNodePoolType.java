/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.strimzi;

import java.util.function.Consumer;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.skodjob.testframe.interfaces.ResourceType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;

/**
 * The type Kafka node pool resource.
 */
public class KafkaNodePoolType implements ResourceType<KafkaNodePool> {
    @Override
    public MixedOperation<KafkaNodePool, KafkaNodePoolList, io.fabric8.kubernetes.client.dsl.Resource<KafkaNodePool>> getClient() {
        return KubeClusterResource.kubeClient().getClient().resources(KafkaNodePool.class, KafkaNodePoolList.class);
    }

    @Override
    public String getKind() {
        return Constants.STRIMZI_KAFKA_NODE_POOL_KIND;
    }

    @Override
    public void create(KafkaNodePool resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaNodePool resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).delete();
    }

    @Override
    public void replace(KafkaNodePool resource, Consumer<KafkaNodePool> editor) {
        KafkaNodePool toBeUpdated = getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get();
        editor.accept(toBeUpdated);
        this.update(toBeUpdated);
    }

    @Override
    public boolean isReady(KafkaNodePool resource) {
        return resource != null;
    }

    @Override
    public boolean isDeleted(KafkaNodePool resource) {
        return resource == null;
    }

    @Override
    public void update(KafkaNodePool resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }
}
