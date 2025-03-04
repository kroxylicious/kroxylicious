/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.vault;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.openshift.api.model.operator.v1.IngressControllerList;
import io.fabric8.openshift.client.OpenShiftClient;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.logs.CollectorElement;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;
import io.kroxylicious.systemtests.utils.TestUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.getInstance;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Vault.
 */
public class Vault {
    public static final String VAULT_SERVICE_NAME = "vault";
    public static final String VAULT_DEFAULT_NAMESPACE = "vault";
    public static final String VAULT_HELM_REPOSITORY_URL = "https://helm.releases.hashicorp.com";
    public static final String VAULT_HELM_REPOSITORY_NAME = "hashicorp";
    public static final String VAULT_HELM_CHART_NAME = "hashicorp/vault";
    private static final Logger LOGGER = LoggerFactory.getLogger(Vault.class);
    private final String deploymentNamespace;
    private final String vaultRootToken;

    /**
     * Instantiates a new Vault.
     *
     * @param vaultRootToken root token to be used for the vault install
     */
    public Vault(String vaultRootToken) {
        this.deploymentNamespace = VAULT_DEFAULT_NAMESPACE;
        this.vaultRootToken = vaultRootToken;
    }

    /**
     * Is deployed
     *
     * @return true if Vault service is deployed in kubernetes, false otherwise
     */
    public boolean isDeployed() {
        return kubeClient().getService(deploymentNamespace, VAULT_SERVICE_NAME) != null;
    }

    /**
     * Deploy.
     *
     */
    public void deploy() {
        if (isDeployed()) {
            LOGGER.warn("Skipping Vault deployment. It is already deployed!");
            return;
        }

        boolean openshiftCluster = getInstance().isOpenshift();
        LOGGER.info("Deploy HashiCorp Vault in {} namespace, openshift: {}", deploymentNamespace, openshiftCluster);

        NamespaceUtils.createNamespaceAndPrepare(deploymentNamespace);
        ResourceManager.helmClient().addRepository(VAULT_HELM_REPOSITORY_NAME, VAULT_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(VAULT_HELM_CHART_NAME, VAULT_SERVICE_NAME,
                Optional.empty(),
                Optional.of(Path.of(TestUtils.getResourcesURI("helm_vault_overrides.yaml"))),
                Optional.of(Map.of("server.image.repository", Constants.DOCKER_REGISTRY_GCR_MIRROR + "/" + VAULT_HELM_CHART_NAME,
                        "server.dev.devRootToken", vaultRootToken,
                        "global.openshift", String.valueOf(openshiftCluster),
                        "server.route.enabled", String.valueOf(openshiftCluster),
                        "server.route.host", VAULT_SERVICE_NAME + "." + getIngressDomain(openshiftCluster),
                        "server.route.tls", "null")));
    }

    private String getIngressDomain(boolean openshiftCluster) {
        String defaultDomain = "local";
        if (openshiftCluster) {
            OpenShiftClient openshiftClient = kubeClient().getClient().adapt(OpenShiftClient.class);
            IngressControllerList pods = openshiftClient.operator().ingressControllers().inNamespace("openshift-ingress-operator").list();
            return pods.getItems().stream().map(x -> x.getStatus().getDomain()).findFirst().orElse(defaultDomain);
        }
        return defaultDomain;
    }

    /**
     * Delete.
     *
     * @throws IOException the io exception
     */
    public void delete() throws IOException {
        LOGGER.info("Deleting Vault in {} namespace", deploymentNamespace);
        String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();
        String testCaseName = ResourceManager.getTestContext().getTestMethod().orElse(null) == null ? ""
                : ResourceManager.getTestContext().getRequiredTestMethod().getName();
        NamespaceUtils.deleteNamespaceWithWaitAndRemoveFromSet(deploymentNamespace, new CollectorElement(testSuiteName, testCaseName));
    }

    /**
     * Gets the vault url.
     *
     * @return the vault url.
     */
    public URI getVaultUrl() {
        if (getInstance().isOpenshift()) {
            return URI.create("http://" + DeploymentUtils.getOpenshiftRouteServiceAddress(deploymentNamespace, VAULT_SERVICE_NAME));
        }
        else {
            return URI.create("http://" + DeploymentUtils.getNodePortServiceAddress(deploymentNamespace, VAULT_SERVICE_NAME));
        }
    }
}
