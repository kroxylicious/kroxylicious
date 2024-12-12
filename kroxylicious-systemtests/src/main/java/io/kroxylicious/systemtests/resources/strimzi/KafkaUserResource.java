/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.strimzi;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserList;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceManager;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kafka user resource.
 */
public class KafkaUserResource implements ResourceType<KafkaUser> {

    @Override
    public String getKind() {
        return Constants.KAFKA_USER_KIND;
    }

    @Override
    public KafkaUser get(String namespace, String name) {
        return kafkaUserClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(KafkaUser resource) {
        kafkaUserClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaUser resource) {
        kafkaUserClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName())
                .withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public boolean waitForReadiness(KafkaUser resource) {
        return ResourceManager.waitForResourceStatusReady(kafkaUserClient(), resource);
    }

    @Override
    public void update(KafkaUser kafkaUser) {
        kafkaUserClient().inNamespace(kafkaUser.getMetadata().getNamespace()).resource(kafkaUser).update();
    }

    /**
     * Kafka user client mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<KafkaUser, KafkaUserList, io.fabric8.kubernetes.client.dsl.Resource<KafkaUser>> kafkaUserClient() {
        return kubeClient().getClient().resources(KafkaUser.class, KafkaUserList.class);
    }
}
