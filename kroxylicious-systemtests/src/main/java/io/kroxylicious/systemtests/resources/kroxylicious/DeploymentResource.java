/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kroxylicious;

import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceOperation;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type deployment resource.
 */
public class DeploymentResource implements ResourceType<Deployment> {
    @Override
    public String getKind() {
        return Constants.DEPLOYMENT;
    }

    @Override
    public Deployment get(String namespace, String name) {
        return deploymentClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(Deployment resource) {
        deploymentClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(Deployment resource) {
        deploymentClient().inNamespace(resource.getMetadata().getNamespace())
                          .withName(
                                  resource.getMetadata().getName()
                          )
                          .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                          .delete();
    }

    @Override
    public void update(Deployment resource) {
        deploymentClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(Deployment resource) {
        deploymentClient().inNamespace(resource.getMetadata().getNamespace())
                          .withName(resource.getMetadata().getName())
                          .waitUntilReady(ResourceOperation.getTimeoutForResourceReadiness(resource.getKind()).toMillis(), TimeUnit.MILLISECONDS);
        return true;
    }

    /**
     * Deployment client mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<Deployment, DeploymentList, Resource<Deployment>> deploymentClient() {
        return kubeClient().getClient().resources(Deployment.class, DeploymentList.class);
    }
}
