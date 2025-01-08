/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.skodjob.testframe.enums.InstallType;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceType;
import io.kroxylicious.systemtests.resources.kroxylicious.DeploymentResource;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.TestUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class BundleResource implements ResourceType<Deployment> {
    public static final String PATH_TO_KO_CONFIG = Constants.PATH_TO_OPERATOR_INSTALL_FILES + "/03.Deployment.kroxylicious-operator.yaml";

    private String name;
    private String namespaceInstallTo;
    private String namespaceToWatch;
    private Duration operationTimeout;
    private Duration reconciliationInterval;
    private Map<String, String> extraLabels;
    private int replicas = 1;

    @Override
    public String getKind() {
        return Constants.DEPLOYMENT;
    }

    @Override
    public Deployment get(String namespace, String name) {
        String deploymentName = kubeClient().getDeploymentNameByPrefix(namespace, name);
        return deploymentName != null ? kubeClient().getDeployment(namespace, deploymentName) : null;
    }

    @Override
    public void create(Deployment resource) {
        kubeClient().createOrReplaceDeployment(resource);
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
        this.namespaceToWatch = builder.namespaceToWatch;
        this.operationTimeout = builder.operationTimeout;
        this.reconciliationInterval = builder.reconciliationInterval;
        this.extraLabels = builder.extraLabels;
        this.replicas = builder.replicas;

        // assign defaults is something is not specified
        if (this.name == null || this.name.isEmpty()) {
            this.name = Constants.KO_DEPLOYMENT_NAME;
        }
        if (this.namespaceToWatch == null) {
            this.namespaceToWatch = this.namespaceInstallTo;
        }
        if (this.operationTimeout.isZero()) {
            this.operationTimeout = Constants.KO_OPERATION_TIMEOUT_DEFAULT;
        }
        if (this.reconciliationInterval.isZero()) {
            this.reconciliationInterval = Constants.RECONCILIATION_INTERVAL;
        }
        if (this.extraLabels == null) {
            this.extraLabels = new HashMap<>();
        }
    }

    public static class BundleResourceBuilder {

        private String name;
        private String namespaceInstallTo;
        private String namespaceToWatch;
        private Duration operationTimeout;
        private Duration reconciliationInterval;
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

        public BundleResourceBuilder withWatchingNamespaces(String namespaceToWatch) {
            this.namespaceToWatch = namespaceToWatch;
            return self();
        }

        public BundleResourceBuilder withOperationTimeout(Duration operationTimeout) {
            this.operationTimeout = operationTimeout;
            return self();
        }

        public BundleResourceBuilder withReconciliationInterval(Duration reconciliationInterval) {
            this.reconciliationInterval = reconciliationInterval;
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

        public BundleResourceBuilder defaultConfigurationWithNamespace(String namespaceName) {
            this.name = Constants.KO_DEPLOYMENT_NAME;
            this.namespaceInstallTo = namespaceName;
            this.namespaceToWatch = this.namespaceInstallTo;
            this.operationTimeout = Constants.KO_OPERATION_TIMEOUT_DEFAULT;
            this.reconciliationInterval = Constants.RECONCILIATION_INTERVAL;
            return self();
        }

        public BundleResource buildBundleInstance() {
            return new BundleResource(this);
        }
    }

    protected BundleResourceBuilder newBuilder() {
        return new BundleResourceBuilder();
    }

    protected BundleResourceBuilder toBuilder() {
        return newBuilder()
                .withName(name)
                .withNamespace(namespaceInstallTo)
                .withWatchingNamespaces(namespaceToWatch)
                .withOperationTimeout(operationTimeout)
                .withReconciliationInterval(reconciliationInterval)
                .withReplicas(replicas);
    }

    public DeploymentBuilder buildBundleDeployment() {
        Deployment kroxyliciousOperator = DeploymentResource.getDeploymentFromYaml(PATH_TO_KO_CONFIG);
        // Get default KO image
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
                .addToMatchLabels("name", Constants.KROXYLICIOUS)
                .addToMatchLabels(this.extraLabels)
                .endSelector()
                .editTemplate()
                .editMetadata()
                .addToLabels("name", Constants.KROXYLICIOUS)
                .addToLabels(this.extraLabels)
                .endMetadata()
                .editSpec()
                .editFirstContainer()
                .withImage(TestUtils.changeOrgAndTag(koImage))
                .withImagePullPolicy(Constants.PULL_IMAGE_IF_NOT_PRESENT)
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec();
    }
}
