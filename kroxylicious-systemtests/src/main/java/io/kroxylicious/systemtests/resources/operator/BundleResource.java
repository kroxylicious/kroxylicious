/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.util.HashMap;
import java.util.Map;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.skodjob.testframe.enums.InstallType;
import io.skodjob.testframe.utils.ImageUtils;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.resources.ResourceType;
import io.kroxylicious.systemtests.resources.kroxylicious.DeploymentResource;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class BundleResource implements ResourceType<Deployment> {

    private String name;
    private String namespaceInstallTo;
    private Map<String, String> extraLabels;
    private int replicas = 1;

    @Override
    public String getKind() {
        return Constants.DEPLOYMENT;
    }

    @Override
    public Deployment get(String namespace, String name) {
        return kubeClient().getDeployment(namespace, name);
    }

    @Override
    public void create(Deployment resource) {
        kubeClient().createOrUpdateDeployment(resource);
    }

    @Override
    public void delete(Deployment resource) {
        kubeClient().deleteDeployment(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    @Override
    public void update(Deployment resource) {
        kubeClient().updateDeployment(resource);
    }

    @Override
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public boolean waitForReadiness(Deployment resource) {
        boolean resourceNonNull = resource != null
                && resource.getMetadata() != null
                && resource.getMetadata().getName() != null
                && resource.getStatus() != null;
        if (resourceNonNull) {
            DeploymentUtils.waitForDeploymentReady(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
            return DeploymentUtils.waitForDeploymentRunning(resource.getMetadata().getNamespace(), resource.getMetadata().getName(),
                    resource.getSpec().getReplicas(), Constants.GLOBAL_STATUS_TIMEOUT);
        }
        return false;
    }

    // this is for resourceTypes inside ResourceManager
    public BundleResource() {
    }

    private BundleResource(BundleResourceBuilder builder) {
        this.name = builder.name;
        this.namespaceInstallTo = builder.namespaceInstallTo;
        this.extraLabels = builder.extraLabels;
        this.replicas = builder.replicas;

        // assign defaults is something is not specified
        if (this.name == null || this.name.isEmpty()) {
            this.name = Constants.KO_DEPLOYMENT_NAME;
        }
        if (this.extraLabels == null) {
            this.extraLabels = new HashMap<>();
        }
    }

    public static class BundleResourceBuilder {

        private String name;
        private String namespaceInstallTo;
        private Map<String, String> extraLabels;
        private int replicas;

        public BundleResourceBuilder withName(String name) {
            this.name = name;
            return self();
        }

        public BundleResourceBuilder withNamespace(String namespaceInstallTo) {
            this.namespaceInstallTo = namespaceInstallTo;
            return self();
        }

        public BundleResourceBuilder withExtraLabels(Map<String, String> extraLabels) {
            this.extraLabels = extraLabels;
            return self();
        }

        public BundleResourceBuilder withReplicas(int replicas) {
            this.replicas = replicas;
            return self();
        }

        protected BundleResourceBuilder self() {
            return this;
        }

        public BundleResource buildBundleInstance() {
            return new BundleResource(this);
        }
    }

    public DeploymentBuilder buildBundleDeployment(String pathToConfig) {
        Deployment kroxyliciousOperator = DeploymentResource.getDeploymentFromYaml(pathToConfig);
        // Get default Kroxylicious Operator image
        String koImage = kroxyliciousOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getImage();

        return new DeploymentBuilder(kroxyliciousOperator)
                .editMetadata()
                .withName(name)
                .withNamespace(namespaceInstallTo)
                .addToLabels(Constants.DEPLOYMENT_TYPE, InstallType.Yaml.name())
                .endMetadata()
                .editSpec()
                .withReplicas(this.replicas)
                .withNewSelector()
                .addToMatchLabels(this.extraLabels)
                .endSelector()
                .editTemplate()
                .editMetadata()
                .addToLabels(this.extraLabels)
                .endMetadata()
                .editSpec()
                .editFirstContainer()
                .withImage(ImageUtils.changeRegistryOrgAndTag(koImage, Environment.KROXY_REGISTRY, Environment.KROXY_ORG, Environment.KROXY_TAG))
                .withImagePullPolicy(Constants.PULL_IMAGE_IF_NOT_PRESENT)
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec();
    }
}
