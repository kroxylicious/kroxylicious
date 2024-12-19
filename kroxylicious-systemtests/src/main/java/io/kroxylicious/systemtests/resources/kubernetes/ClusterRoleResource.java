/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class ClusterRoleResource implements ResourceType<ClusterRole> {

    @Override
    public String getKind() {
        return Constants.CLUSTER_ROLE;
    }

    @Override
    public ClusterRole get(String namespace, String name) {
        return kubeClient(KubeClusterResource.getInstance().defaultNamespace()).getClusterRole(name);
    }

    @Override
    public void create(ClusterRole resource) {
        // ClusterRole his operation namespace is only 'default'
        kubeClient(KubeClusterResource.getInstance().defaultNamespace()).createOrUpdateClusterRoles(resource);
    }

    @Override
    public void delete(ClusterRole resource) {
        // ClusterRole his operation namespace is only 'default'
        kubeClient(KubeClusterResource.getInstance().defaultNamespace()).deleteClusterRole(resource);
    }

    @Override
    public void update(ClusterRole resource) {
        kubeClient(KubeClusterResource.getInstance().defaultNamespace()).createOrUpdateClusterRoles(resource);
    }

    @Override
    public boolean waitForReadiness(ClusterRole resource) {
        return resource != null;
    }
}
