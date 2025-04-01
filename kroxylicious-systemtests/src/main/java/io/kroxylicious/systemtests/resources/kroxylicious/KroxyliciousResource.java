/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kroxylicious;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.resources.ResourceType;

public class KroxyliciousResource<T extends HasMetadata> implements ResourceType<T> {
    private final Class<T> resourceClass;

    public KroxyliciousResource(Class<T> resourceClass) {
        this.resourceClass = resourceClass;
    }

    /**
     * Kafka Proxy mixed operation.
     *
     * @return the mixed operation
     */
    public MixedOperation<T, KubernetesResourceList<T>, Resource<T>> kubeClient() {
        return KubeClusterResource.kubeClient().getClient().resources(resourceClass);
    }

    @Override
    public String getKind() {
        return resourceClass.getSimpleName();
    }

    @Override
    public T get(String namespace, String name) {
        return kubeClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(T resource) {
        kubeClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(T resource) {
        kubeClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(T resource) {
        kubeClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(T resource) {
        return resource != null;
    }
}
