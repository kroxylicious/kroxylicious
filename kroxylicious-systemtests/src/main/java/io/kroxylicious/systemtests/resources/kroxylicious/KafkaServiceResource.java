/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kroxylicious;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class KafkaServiceResource implements ResourceType<KafkaService> {

    @Override
    public String getKind() {
        return Constants.KAFKA_SERVICE_KIND;
    }

    @Override
    public KafkaService get(String namespace, String name) {
        return kafkaServiceClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(KafkaService resource) {
        kafkaServiceClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaService resource) {
        kafkaServiceClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(KafkaService resource) {
        kafkaServiceClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(KafkaService resource) {
        return resource != null;
    }

    /**
     * Kafka Service mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<KafkaService, KubernetesResourceList<KafkaService>, Resource<KafkaService>> kafkaServiceClient() {
        return kubeClient().getClient().resources(KafkaService.class);
    }
}
