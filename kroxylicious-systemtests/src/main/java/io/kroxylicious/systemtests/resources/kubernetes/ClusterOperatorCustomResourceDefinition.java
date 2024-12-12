/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kubernetes;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class ClusterOperatorCustomResourceDefinition implements ResourceType<CustomResourceDefinition> {

    @Override
    public String getKind() {
        return Constants.CUSTOM_RESOURCE_DEFINITION;
    }
    @Override
    public CustomResourceDefinition get(String namespace, String name) {
        return kubeClient().getCustomResourceDefinition(name);
    }
    @Override
    public void create(CustomResourceDefinition resource) {
        kubeClient().createOrUpdateCustomResourceDefinition(resource);
    }
    @Override
    public void delete(CustomResourceDefinition resource) {
        kubeClient().deleteCustomResourceDefinition(resource);
    }

    @Override
    public void update(CustomResourceDefinition resource) {
        kubeClient().createOrUpdateCustomResourceDefinition(resource);
    }

    @Override
    public boolean waitForReadiness(CustomResourceDefinition resource) {
        return resource != null;
    }
}
