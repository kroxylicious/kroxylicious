/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.kroxylicious.systemtest.resources.crd;

import java.util.function.Consumer;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;

import io.kroxylicious.systemtest.enums.CustomResourceStatus;
import io.kroxylicious.systemtest.resources.ResourceManager;
import io.kroxylicious.systemtest.resources.ResourceOperation;
import io.kroxylicious.systemtest.resources.ResourceType;

public class KafkaTopicResource implements ResourceType<KafkaTopic> {

    public KafkaTopicResource() {
    }

    @Override
    public String getKind() {
        return KafkaTopic.RESOURCE_KIND;
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
        return ResourceManager.waitForResourceStatus(kafkaTopicClient(), resource.getKind(), resource.getMetadata().getNamespace(),
                resource.getMetadata().getName(), CustomResourceStatus.Ready, ResourceOperation.getTimeoutForResourceReadiness(resource.getKind()));
    }

    public static MixedOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> kafkaTopicClient() {
        return Crds.topicOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceTopicResourceInSpecificNamespace(String resourceName, Consumer<KafkaTopic> editor, String namespaceName) {
        ResourceManager.replaceCrdResource(KafkaTopic.class, KafkaTopicList.class, resourceName, editor, namespaceName);
    }
}
