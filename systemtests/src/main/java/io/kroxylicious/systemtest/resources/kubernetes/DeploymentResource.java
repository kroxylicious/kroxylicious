/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.kroxylicious.systemtest.resources.kubernetes;

import java.util.function.Consumer;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.strimzi.test.TestUtils;

import io.kroxylicious.systemtest.Constants;
import io.kroxylicious.systemtest.resources.ResourceManager;
import io.kroxylicious.systemtest.resources.ResourceType;
import io.kroxylicious.systemtest.utils.kubeUtils.controllers.DeploymentUtils;

public class DeploymentResource implements ResourceType<Deployment> {

    @Override
    public String getKind() {
        return Constants.DEPLOYMENT;
    }

    @Override
    public Deployment get(String namespace, String name) {
        return ResourceManager.kubeClient().getDeployment(namespace, name);
    }

    @Override
    public void create(Deployment resource) {
        ResourceManager.kubeClient().createDeployment(resource);
    }

    @Override
    public void delete(Deployment resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteDeployment(resource.getMetadata().getNamespace(),
                resource.getMetadata().getName());
    }

    @Override
    public void update(Deployment resource) {
        ResourceManager.kubeClient().updateDeployment(resource);
    }

    @Override
    public boolean waitForReadiness(Deployment resource) {
        return DeploymentUtils.waitForDeploymentAndPodsReady(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), resource.getSpec().getReplicas());
    }

    public static void replaceDeployment(String deploymentName, Consumer<Deployment> editor, String namespaceName) {
        Deployment toBeReplaced = ResourceManager.kubeClient().getClient().resources(Deployment.class, DeploymentList.class).inNamespace(namespaceName)
                .withName(deploymentName).get();
        editor.accept(toBeReplaced);
        ResourceManager.kubeClient().getClient().resources(Deployment.class, DeploymentList.class).inNamespace(namespaceName).resource(toBeReplaced).update();
    }

    public static Deployment getDeploymentFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Deployment.class);
    }
}
