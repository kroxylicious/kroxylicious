/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.kroxylicious.systemtest.resources.draincleaner;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.test.TestUtils;

import io.kroxylicious.systemtest.Constants;
import io.kroxylicious.systemtest.enums.DeploymentTypes;
import io.kroxylicious.systemtest.resources.ResourceManager;
import io.kroxylicious.systemtest.resources.ResourceType;
import io.kroxylicious.systemtest.resources.kubernetes.DeploymentResource;
import io.kroxylicious.systemtest.utils.kubeUtils.controllers.DeploymentUtils;

public class DrainCleanerResource implements ResourceType<Deployment> {
    public static final String PATH_TO_DRAIN_CLEANER_DEP = TestUtils.USER_PATH + "/../packaging/install/drain-cleaner/kubernetes/060-Deployment.yaml";

    @Override
    public String getKind() {
        return Constants.DEPLOYMENT;
    }

    @Override
    public Deployment get(String namespace, String name) {
        String deploymentName = ResourceManager.kubeClient().namespace(namespace).getDeploymentNameByPrefix(name);
        return deploymentName != null ? ResourceManager.kubeClient().getDeployment(namespace, deploymentName) : null;
    }

    @Override
    public void create(Deployment resource) {
        ResourceManager.kubeClient().createDeployment(resource);
    }

    @Override
    public void delete(Deployment resource) {
        ResourceManager.kubeClient().deleteDeployment(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    @Override
    public void update(Deployment resource) {
        ResourceManager.kubeClient().updateDeployment(resource);
    }

    @Override
    public boolean waitForReadiness(Deployment resource) {
        return resource != null
                && resource.getMetadata() != null
                && resource.getMetadata().getName() != null
                && resource.getStatus() != null
                && DeploymentUtils.waitForDeploymentAndPodsReady(resource.getMetadata().getNamespace(), resource.getMetadata().getName(),
                        resource.getSpec().getReplicas());
    }

    public DeploymentBuilder buildDrainCleanerDeployment() {
        Deployment drainCleaner = DeploymentResource.getDeploymentFromYaml(PATH_TO_DRAIN_CLEANER_DEP);

        return new DeploymentBuilder(drainCleaner)
                .editOrNewMetadata()
                .addToLabels(Constants.DEPLOYMENT_TYPE, DeploymentTypes.DrainCleaner.name())
                .endMetadata();
    }
}
