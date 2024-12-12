/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kubernetes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.resources.ResourceManager;
import io.kroxylicious.systemtests.resources.ResourceType;
import io.kroxylicious.systemtests.utils.ReadWriteUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class ClusterRoleBindingResource implements ResourceType<ClusterRoleBinding> {

    private static final Logger LOGGER = LogManager.getLogger(ClusterRoleBindingResource.class);

    @Override
    public String getKind() {
        return Constants.CLUSTER_ROLE_BINDING;
    }

    @Override
    public ClusterRoleBinding get(String namespace, String name) {
        // ClusterRoleBinding his operation namespace is only 'default'
        return kubeClient(KubeClusterResource.getInstance().getNamespace()).getClusterRoleBinding(name);
    }

    @Override
    public void create(ClusterRoleBinding resource) {
        kubeClient(KubeClusterResource.getInstance().getNamespace()).createOrUpdateClusterRoleBinding(resource);
    }

    @Override
    public void delete(ClusterRoleBinding resource) {
        // ClusterRoleBinding his operation namespace is only 'default'
        kubeClient(KubeClusterResource.getInstance().getNamespace()).deleteClusterRoleBinding(resource);
    }

    @Override
    public void update(ClusterRoleBinding resource) {
        kubeClient(KubeClusterResource.getInstance().getNamespace()).createOrUpdateClusterRoleBinding(resource);
    }

    @Override
    public boolean waitForReadiness(ClusterRoleBinding resource) {
        return resource != null;
    }

    public static ClusterRoleBinding clusterRoleBinding(String namespace, String yamlPath) {
        LOGGER.info("Creating ClusterRoleBinding in test case {} from {} in Namespace: {}",
                ResourceManager.getTestContext().getDisplayName(), yamlPath, namespace);
        ClusterRoleBinding clusterRoleBinding = getClusterRoleBindingFromYaml(yamlPath);
        clusterRoleBinding = new ClusterRoleBindingBuilder(clusterRoleBinding)
                .editFirstSubject()
                .withNamespace(namespace)
                .endSubject().build();

        ResourceManager.getInstance().createResourceWithWait(clusterRoleBinding);

        return clusterRoleBinding;
    }

    public static ClusterRoleBinding clusterRoleBinding(ClusterRoleBinding clusterRoleBinding) {
        ResourceManager.getInstance().createResourceWithWait(clusterRoleBinding);
        return clusterRoleBinding;
    }

    private static ClusterRoleBinding getClusterRoleBindingFromYaml(String yamlPath) {
        return ReadWriteUtils.readObjectFromYamlFilepath(yamlPath, ClusterRoleBinding.class);
    }
}
