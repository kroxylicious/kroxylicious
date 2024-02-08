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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.kroxylicious.ConfigMapResource;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Vault.
 */
public class Vault {
    private static final Logger LOGGER = LoggerFactory.getLogger(Vault.class);
    private static final String VAULT_CMD = "vault";
    public static final String VAULT_SERVICE_NAME = "vault";
    public static final String VAULT_POD_NAME = VAULT_SERVICE_NAME + "-0";
    public static final String VAULT_DEFAULT_NAMESPACE = "vault";
    public static final String VAULT_ROOT_TOKEN = "myRootToken";
    public static final String VAULT_HELM_REPOSITORY_URL = "https://helm.releases.hashicorp.com";
    public static final String VAULT_HELM_REPOSITORY_NAME = "hashicorp";
    public static final String VAULT_HELM_CHART_NAME = "hashicorp/vault";
    private final String deploymentNamespace;

    /**
     * Instantiates a new Vault.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public Vault(String deploymentNamespace) {
        this.deploymentNamespace = deploymentNamespace;
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
     * Deploy.
     *
     */
    public void deploy() {
        LOGGER.info("Deploy HashiCorp Vault in {} namespace", deploymentNamespace);
        if (isDeployed()) {
            LOGGER.warn("Skipping Vault deployment. It is already deployed!");
            return;
        }

        var cm = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("kroxylicious-encryption-filter-policy")
                .endMetadata()
                .addToData("kroxylicious_encryption_filter_policy.hcl", readResource("kroxylicious_encryption_filter_policy.hcl")).build();

        ConfigMapResource.configClient()
                .inNamespace(deploymentNamespace)
                .resource(cm)
                .create();

        ResourceManager.helmClient().addRepository(VAULT_HELM_REPOSITORY_NAME, VAULT_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(VAULT_HELM_CHART_NAME, VAULT_SERVICE_NAME, VAULT_ROOT_TOKEN, "latest", getHelmOverridePath());

        DeploymentUtils.waitForDeploymentRunning(deploymentNamespace, VAULT_POD_NAME, Duration.ofMinutes(1));

        configureVault(deploymentNamespace);
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

    private void configureVault(String deploymentNamespace) {
        LOGGER.info("Enabling transit in vault instance");
        String loginCommand = VAULT_CMD + " login " + VAULT_ROOT_TOKEN;
        String transitCommand = VAULT_CMD + " secrets enable transit";

        try (var error = new ByteArrayOutputStream();
                var exec = kubeClient().getClient().pods()
                        .inNamespace(deploymentNamespace)
                        .withName(VAULT_POD_NAME)
                        .writingError(error)
                        .exec("sh", "-c", String.format("%s && %s", loginCommand, transitCommand))) {
            int exitCode = exec.exitCode().join();
            if (exitCode != 0) {
                throw new KubeClusterException(String.format("Cannot enable transit in vault instance! Error: %s", error));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
     * Gets bootstrap.
     *
     * @return the bootstrap
     */
    public String getVaultUrl() {
        String clusterIP = kubeClient().getService(deploymentNamespace, VAULT_SERVICE_NAME).getSpec().getClusterIP();
        if (clusterIP == null || clusterIP.isEmpty()) {
            throw new KubeClusterException("Unable to get the clusterIP of Vault");
        }
        int port = kubeClient().getService(deploymentNamespace, VAULT_SERVICE_NAME).getSpec().getPorts().get(0).getPort();
        String bootstrap = clusterIP + ":" + port;
        LOGGER.debug("Vault bootstrap: {}", bootstrap);
        return bootstrap;
    }

    private String readResource(String resource) {
        try (var inputStream = this.getClass().getResourceAsStream(resource)) {
            if (inputStream == null) {
                throw new IllegalStateException("Failed to find resource: %s ".formatted(resource));
            }
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read resource: %s".formatted(resource), e);
        }
    }
}
