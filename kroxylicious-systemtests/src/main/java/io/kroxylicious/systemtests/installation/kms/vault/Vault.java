/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.vault;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.openshift.api.model.operator.v1.IngressControllerList;
import io.fabric8.openshift.client.OpenShiftClient;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;
import io.kroxylicious.systemtests.utils.TestUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.getInstance;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Vault.
 */
public class Vault {
    public static final String VAULT_SERVICE_NAME = "vault";
    public static final String VAULT_POD_NAME = VAULT_SERVICE_NAME + "-0";
    public static final String VAULT_DEFAULT_NAMESPACE = "vault";
    public static final String VAULT_HELM_REPOSITORY_URL = "https://helm.releases.hashicorp.com";
    public static final String VAULT_HELM_REPOSITORY_NAME = "hashicorp";
    public static final String VAULT_HELM_CHART_NAME = "hashicorp/vault";
    private static final Logger LOGGER = LoggerFactory.getLogger(Vault.class);
    private static final String VAULT_CMD = "vault";
    private final String deploymentNamespace;
    private final String vaultRootToken;
    private String version;

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
     * Gets the installed version.
     *
     * @return the version
     */
    public String getVersionInstalled() {
        if (version == null || version.isEmpty()) {
            List<String> command = List.of(VAULT_CMD, "version");
            ExecResult execResult = cmdKubeClient(deploymentNamespace).execInPod(VAULT_POD_NAME, true, command);

            if (!execResult.isSuccess()) {
                throw new KubeClusterException("Failed to run Vault: %s, exit code: %d, stderr: %s".formatted(String.join(" ", command),
                        execResult.returnCode(), execResult.err()));
            }
            // version returned with format: Vault v1.15.2 (blah blah), build blah
            version = execResult.out().trim().split("\\s+")[1].replace("v", "");
            if (!version.matches("^(\\d+)(?:\\.(\\d+))?(?:\\.(\\*|\\d+))?$")) {
                throw new NumberFormatException("Invalid version format: " + version);
            }
        }
        return version;
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

        NamespaceUtils.createNamespaceWithWait(deploymentNamespace);
        ResourceManager.helmClient().addRepository(VAULT_HELM_REPOSITORY_NAME, VAULT_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(VAULT_HELM_CHART_NAME, VAULT_SERVICE_NAME,
                Optional.of(Environment.VAULT_CHART_VERSION),
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
        NamespaceUtils.deleteNamespaceWithWait(deploymentNamespace);
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
