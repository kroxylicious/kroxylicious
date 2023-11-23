/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.strimzi;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.strimzi.api.kafka.KafkaNodePoolList;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kafka node pool resource.
 */
public class KafkaNodePoolResource implements ResourceType<KafkaNodePool> {
    @Override
    public String getKind() {
        return Constants.KAFKA_NODE_POOL_KIND;
    }

    @Override
    public KafkaNodePool get(String namespace, String name) {
        return kafkaNodePoolClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(KafkaNodePool resource) {
        kafkaNodePoolClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaNodePool resource) {
        kafkaNodePoolClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).delete();
    }

    @Override
    public void update(KafkaNodePool resource) {
        kafkaNodePoolClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(KafkaNodePool resource) {
        return resource != null;
    }

    /**
     * Kafka node pool client mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<KafkaNodePool, KafkaNodePoolList, io.fabric8.kubernetes.client.dsl.Resource<KafkaNodePool>> kafkaNodePoolClient() {
        return kubeClient().getClient().resources(KafkaNodePool.class, KafkaNodePoolList.class);
    }
}
