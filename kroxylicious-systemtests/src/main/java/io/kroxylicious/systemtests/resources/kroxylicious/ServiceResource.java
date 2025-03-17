/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kroxylicious;

import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceOperation;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type service resource.
 */
public class ServiceResource implements ResourceType<Service> {
    @Override
    public String getKind() {
        return Constants.SERVICE;
    }

    @Override
    public Service get(String namespace, String name) {
        return serviceClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(Service resource) {
        serviceClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(Service resource) {
        serviceClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(Service resource) {
        serviceClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(Service resource) {
        serviceClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName())
                .waitUntilReady(ResourceOperation.getTimeoutForResourceReadiness(resource.getKind()).toMillis(), TimeUnit.MILLISECONDS);
        return true;
    }

    /**
     * service client mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<Service, ServiceList, Resource<Service>> serviceClient() {
        return kubeClient().getClient().resources(Service.class, ServiceList.class);
    }
}
