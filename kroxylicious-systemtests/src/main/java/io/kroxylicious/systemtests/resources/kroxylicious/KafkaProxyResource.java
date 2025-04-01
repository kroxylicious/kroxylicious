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

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class KafkaProxyResource implements ResourceType<KafkaProxy> {

    @Override
    public String getKind() {
        return Constants.KAFKA_PROXY_KIND;
    }

    @Override
    public KafkaProxy get(String namespace, String name) {
        return kafkaProxyClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(KafkaProxy resource) {
        kafkaProxyClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaProxy resource) {
        kafkaProxyClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(KafkaProxy resource) {
        kafkaProxyClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(KafkaProxy resource) {
        return resource != null;
    }

    /**
     * Kafka Proxy mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> kafkaProxyClient() {
        return kubeClient().getClient().resources(KafkaProxy.class);
    }
}
