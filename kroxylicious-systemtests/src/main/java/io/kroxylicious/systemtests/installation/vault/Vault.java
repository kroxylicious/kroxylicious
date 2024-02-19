/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.vault;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ServicePort;

import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

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

    /**
     * Instantiates a new Vault.
     *
     * @param deploymentNamespace the deployment namespace
     * @param vaultRootToken root token to be used for the vault install
     */
    public Vault(String deploymentNamespace, String vaultRootToken) {
        this.deploymentNamespace = deploymentNamespace;
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
     * Is available.
     *
     * @return true if Vault service is available in kubernetes, false otherwise
     */
    public boolean isAvailable() {
        if (!isDeployed()) {
            return false;
        }
        try (var output = new ByteArrayOutputStream();
                var exec = kubeClient().getClient().pods()
                        .inNamespace(deploymentNamespace)
                        .withName(VAULT_POD_NAME)
                        .writingOutput(output)
                        .exec("sh", "-c", VAULT_CMD + " operator init -status")) {
            int exitCode = exec.exitCode().join();
            return exitCode == 0 &&
                    output.toString().toLowerCase().contains("vault is initialized");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Gets the installed version.
     *
     * @return the version
     */
    public String getVersionInstalled() {
        try (var output = new ByteArrayOutputStream();
                var exec = kubeClient().getClient().pods()
                        .inNamespace(deploymentNamespace)
                        .withName(VAULT_POD_NAME)
                        .writingOutput(output)
                        .exec("sh", "-c", VAULT_CMD + " version")) {
            exec.exitCode().join();
            // version returned as: Vault v1.15.2 (blah blah), build blah
            return output.toString().split(" ")[1].replace("v", "");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Deploy.
     *
     */
    public void deploy() {
        LOGGER.info("Deploy HashiCorp Vault in {} namespace", deploymentNamespace);
        if (isDeployed()) {
            LOGGER.warn("Skipping Vault deployment. It is already deployed!");
            return;
        }

        ResourceManager.helmClient().addRepository(VAULT_HELM_REPOSITORY_NAME, VAULT_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(VAULT_HELM_CHART_NAME, VAULT_SERVICE_NAME,
                Optional.of(Environment.VAULT_CHART_VERSION),
                Optional.of(getHelmOverridePath()),
                Optional.of(Map.of("server.dev.devRootToken", vaultRootToken)));

        DeploymentUtils.waitForDeploymentRunning(deploymentNamespace, VAULT_POD_NAME, Duration.ofMinutes(1));
    }

    @NonNull
    private Path getHelmOverridePath() {
        var name = "helm_vault_overrides.yaml";
        Path overrideFile;
        var resource = getClass().getResource(name);
        try {
            if (resource == null) {
                throw new IllegalStateException("Cannot find override resource " + name + " on classpath");
            }
            overrideFile = Path.of(resource.toURI());
        }
        catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot determine file system path for " + resource);
        }
        return overrideFile;
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
    public String getVaultUrl() {
        var spec = kubeClient().getService(deploymentNamespace, VAULT_SERVICE_NAME).getSpec();
        String clusterIP = spec.getClusterIP();
        if (clusterIP == null || clusterIP.isEmpty()) {
            throw new KubeClusterException("Unable to get the clusterIP of Vault");
        }
        int port = spec.getPorts().stream().map(ServicePort::getPort).findFirst()
                .orElseThrow(() -> new KubeClusterException("Unable to get the service port of Vault"));
        String url = clusterIP + ":" + port;
        LOGGER.debug("Vault URL: {}", url);
        return url;
    }
}
