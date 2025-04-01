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

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class KafkaProxyIngressResource implements ResourceType<KafkaProxyIngress> {

    @Override
    public String getKind() {
        return Constants.KAFKA_PROXY_INGRESS_KIND;
    }

    @Override
    public KafkaProxyIngress get(String namespace, String name) {
        return kafkaProxyIngressClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(KafkaProxyIngress resource) {
        kafkaProxyIngressClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaProxyIngress resource) {
        kafkaProxyIngressClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(KafkaProxyIngress resource) {
        kafkaProxyIngressClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(KafkaProxyIngress resource) {
        return resource != null;
    }

    /**
     * Kafka Proxy mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<KafkaProxyIngress, KubernetesResourceList<KafkaProxyIngress>, Resource<KafkaProxyIngress>> kafkaProxyIngressClient() {
        return kubeClient().getClient().resources(KafkaProxyIngress.class);
    }
}
