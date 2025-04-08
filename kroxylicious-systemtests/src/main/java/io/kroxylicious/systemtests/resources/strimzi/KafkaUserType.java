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
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserList;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;

/**
 * The type Kafka user resource.
 */
public class KafkaUserType implements ResourceType<KafkaUser> {

    @Override
    public MixedOperation<KafkaUser, KafkaUserList, io.fabric8.kubernetes.client.dsl.Resource<KafkaUser>> getClient() {
        return KubeClusterResource.kubeClient().getClient().resources(KafkaUser.class, KafkaUserList.class);
    }

    @Override
    public String getKind() {
        return Constants.STRIMZI_KAFKA_USER_KIND;
    }

    @Override
    public void create(KafkaUser resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaUser resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName())
                .withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void replace(KafkaUser resource, Consumer<KafkaUser> editor) {
        KafkaUser toBeUpdated = getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get();
        editor.accept(toBeUpdated);
        this.update(toBeUpdated);
    }

    @Override
    public boolean isReady(KafkaUser resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).isReady();
    }

    @Override
    public boolean isDeleted(KafkaUser resource) {
        return resource == null;
    }

    @Override
    public void update(KafkaUser kafkaUser) {
        getClient().inNamespace(kafkaUser.getMetadata().getNamespace()).resource(kafkaUser).update();
    }
}
