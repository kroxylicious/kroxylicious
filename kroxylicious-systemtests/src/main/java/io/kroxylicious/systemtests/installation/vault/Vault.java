/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.vault;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Vault.
 */
public class Vault {
    private static final Logger LOGGER = LoggerFactory.getLogger(Vault.class);
    private static final String VAULT_CMD = "vault";
    public static String VAULT_SERVICE_NAME = "vault";
    public static String VAULT_POD_NAME = VAULT_SERVICE_NAME + "-0";
    public static String VAULT_DEFAULT_NAMESPACE = "vault";
    public static String VAULT_ROOT_TOKEN = "myRootToken";
    public static String VAULT_HELM_REPOSITORY_URL = "https://helm.releases.hashicorp.com";
    public static String VAULT_HELM_REPOSITORY_NAME = "hashicorp";
    public static String VAULT_HELM_CHART_NAME = "hashicorp/vault";
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
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int exitCode = kubeClient().getClient().pods()
                .inNamespace(deploymentNamespace)
                .withName(VAULT_POD_NAME).writingOutput(output)
                .exec("sh", "-c", VAULT_CMD + " operator init -status").exitCode().join();

        return exitCode == 0 &&
                output.toString().toLowerCase().contains("vault is initialized");
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

        Map<String, String> values = new HashMap<>();
        // server
        values.put("server.dev.enabled", "true");
        values.put("server.dev.devRootToken", VAULT_ROOT_TOKEN);
        values.put("server.ha.enabled", "false");
        values.put("server.updateStrategyType", "RollingUpdate");
        values.put("server.service.type", "NodePort");
        // injector
        values.put("injector.enabled", "false");

        ResourceManager.helmClient().addRepository(VAULT_HELM_REPOSITORY_NAME, VAULT_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(VAULT_HELM_CHART_NAME, VAULT_SERVICE_NAME, "latest", values);

        DeploymentUtils.waitForDeploymentRunning(deploymentNamespace, VAULT_POD_NAME, Duration.ofMinutes(1));

        configureVault(deploymentNamespace);
    }

    private void configureVault(String deploymentNamespace) {
        LOGGER.info("Enabling transit in vault instance");
        String loginCommand = VAULT_CMD + " login " + VAULT_ROOT_TOKEN;
        String transitCommand = VAULT_CMD + " secrets enable transit";

        int exitCode = kubeClient().getClient().pods()
                .inNamespace(deploymentNamespace)
                .withName(VAULT_POD_NAME)
                .exec("sh", "-c", String.format("%s && %s", loginCommand, transitCommand)).exitCode().join();

        if (exitCode != 0) {
            String errorLog = kubeClient().logsInSpecificNamespace(deploymentNamespace, VAULT_POD_NAME);
            throw new KubeClusterException(String.format("Cannot enable transit in vault instance! Error: %s", errorLog));
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
    public String getBootstrap() {
        String clusterIP = kubeClient().getService(deploymentNamespace, VAULT_SERVICE_NAME).getSpec().getClusterIP();
        if (clusterIP == null || clusterIP.isEmpty()) {
            throw new KubeClusterException("Unable to get the clusterIP of Vault");
        }
        int port = kubeClient().getService(deploymentNamespace, VAULT_SERVICE_NAME).getSpec().getPorts().get(0).getPort();
        String bootstrap = clusterIP + ":" + port;
        LOGGER.debug("Vault bootstrap: {}", bootstrap);
        return bootstrap;
    }
}
