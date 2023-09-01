/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.kroxylicious.systemtest.resources.kubernetes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.strimzi.test.TestUtils;

import io.kroxylicious.systemtest.Constants;
import io.kroxylicious.systemtest.resources.ResourceManager;
import io.kroxylicious.systemtest.resources.ResourceType;

public class RoleResource implements ResourceType<Role> {

    private static final Logger LOGGER = LogManager.getLogger(RoleResource.class);

    @Override
    public String getKind() {
        return Constants.ROLE;
    }

    @Override
    public Role get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getRole(name);
    }

    @Override
    public void create(Role resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).createOrUpdateRole(resource);
    }

    @Override
    public void delete(Role resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteRole(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    @Override
    public void update(Role resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).createOrUpdateRole(resource);
    }

    @Override
    public boolean waitForReadiness(Role resource) {
        return resource != null && get(resource.getMetadata().getNamespace(), resource.getMetadata().getName()) != null;
    }

    public static void role(ExtensionContext extensionContext, String yamlPath, String namespace) {
        LOGGER.info("Creating Role: {}/{}", namespace, yamlPath);
        Role role = getRoleFromYaml(yamlPath);

        ResourceManager.getInstance().createResourceWithWait(extensionContext, new RoleBuilder(role)
                .editMetadata()
                .withNamespace(namespace)
                .endMetadata()
                .build());
    }

    private static Role getRoleFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Role.class);
    }
}
