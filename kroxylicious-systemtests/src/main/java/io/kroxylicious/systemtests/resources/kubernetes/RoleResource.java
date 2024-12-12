/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kubernetes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceManager;
import io.kroxylicious.systemtests.resources.ResourceType;
import io.kroxylicious.systemtests.utils.ReadWriteUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class RoleResource implements ResourceType<Role> {

    private static final Logger LOGGER = LogManager.getLogger(RoleResource.class);

    @Override
    public String getKind() {
        return Constants.ROLE;
    }

    @Override
    public Role get(String namespace, String name) {
        return kubeClient().namespace(namespace).getRole(name);
    }

    @Override
    public void create(Role resource) {
        kubeClient().namespace(resource.getMetadata().getNamespace()).createOrUpdateRole(resource);
    }

    @Override
    public void delete(Role resource) {
        kubeClient().namespace(resource.getMetadata().getNamespace()).deleteRole(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    @Override
    public void update(Role resource) {
        kubeClient().namespace(resource.getMetadata().getNamespace()).createOrUpdateRole(resource);
    }

    @Override
    public boolean waitForReadiness(Role resource) {
        return resource != null && get(resource.getMetadata().getNamespace(), resource.getMetadata().getName()) != null;
    }

    public static void role(String namespace, String yamlPath) {
        LOGGER.info("Creating Role: {}/{}", namespace, yamlPath);
        Role role = getRoleFromYaml(yamlPath);

        ResourceManager.getInstance().createResourceWithWait(new RoleBuilder(role)
                .editMetadata()
                .withNamespace(namespace)
                .endMetadata()
                .build());
    }

    private static Role getRoleFromYaml(String yamlPath) {
        return ReadWriteUtils.readObjectFromYamlFilepath(yamlPath, Role.class);
    }
}
