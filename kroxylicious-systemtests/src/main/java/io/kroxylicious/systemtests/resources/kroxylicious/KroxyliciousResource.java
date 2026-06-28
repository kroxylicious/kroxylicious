/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kroxylicious;

import java.util.function.Consumer;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.skodjob.testframe.interfaces.ResourceType;

import io.kroxylicious.systemtests.k8s.KubeClusterResource;

public class KroxyliciousResource<T extends HasMetadata> implements ResourceType<T> {
    private final Class<T> resourceClass;

    public KroxyliciousResource(Class<T> resourceClass) {
        this.resourceClass = resourceClass;
    }

    @Override
    public MixedOperation<T, KubernetesResourceList<T>, Resource<T>> getClient() {
        return KubeClusterResource.kubeClient().getClient().resources(resourceClass);
    }

    @Override
    public String getKind() {
        return resourceClass.getSimpleName();
    }

    @Override
    public void create(T resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(T resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void replace(T resource, Consumer<T> editor) {
        T toBeUpdated = getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get();
        editor.accept(toBeUpdated);
        this.update(toBeUpdated);
    }

    @Override
    public boolean isReady(T resource) {
        // Avoiding this warning message as we don't make use of the proposed resources:
        // XXX is not a Readiable resource. It needs to be one of [Node, Deployment, ReplicaSet, StatefulSet, Pod, ReplicationController]
        return resource != null;
    }

    @Override
    public boolean isDeleted(T resource) {
        return resource == null;
    }

    @Override
    public void update(T resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }
}
