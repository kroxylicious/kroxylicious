/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.vault;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
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
     * Is available
     *
     * @return true if Vault service is available in kubernetes, false otherwise
     */
    public boolean isAvailable() {
        return kubeClient().getDeployment(deploymentNamespace, Constants.VAULT_SERVICE_NAME) != null;
    }

    /**
     * Deploy.
     *
     */
    public void deploy() {
        LOGGER.info("Deploy HashiCorp Vault in {} namespace", deploymentNamespace);
        if (isAvailable()) {
            LOGGER.warn("Skipping Vault deployment. It is already deployed!");
            return;
        }

        Map<String, String> values = new HashMap<>();
        // server
        values.put("server.dev.enabled", "true");
        values.put("server.dev.devRootToken", Constants.VAULT_ROOT_TOKEN);
        values.put("server.ha.enabled", "false");
        values.put("server.updateStrategyType", "RollingUpdate");
        values.put("server.service.type", "NodePort");
        // injector
        values.put("injector.enabled", "false");

        ResourceManager.helmClient().addRepository(Constants.VAULT_HELM_REPOSITORY_NAME, Constants.VAULT_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(Constants.VAULT_HELM_CHART_NAME, Constants.VAULT_SERVICE_NAME, "latest", values);

        DeploymentUtils.waitForDeploymentRunning(deploymentNamespace, Constants.VAULT_POD_NAME, Duration.ofMinutes(1));

        configureVault(deploymentNamespace);
    }

    private void configureVault(String deploymentNamespace) {
        LOGGER.info("Enabling transit in vault instance");
        String loginCommand = VAULT_CMD + " login " + Constants.VAULT_ROOT_TOKEN;
        String transitCommand = VAULT_CMD + " secrets enable transit";

        int exitCode = kubeClient().getClient().pods()
                .inNamespace(deploymentNamespace)
                .withName(Constants.VAULT_POD_NAME)
                .exec("sh", "-c", String.format("%s && %s", loginCommand, transitCommand)).exitCode().join();

        if (exitCode != 0) {
            String errorLog = kubeClient().logsInSpecificNamespace(deploymentNamespace, Constants.VAULT_POD_NAME);
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
        String clusterIP = kubeClient().getService(deploymentNamespace, Constants.VAULT_SERVICE_NAME).getSpec().getClusterIP();
        if (clusterIP == null || clusterIP.isEmpty()) {
            throw new KubeClusterException("Unable to get the clusterIP of Vault");
        }
        int port = kubeClient().getService(deploymentNamespace, Constants.VAULT_SERVICE_NAME).getSpec().getPorts().get(0).getPort();
        String bootstrap = clusterIP + ":" + port;
        LOGGER.debug("Vault bootstrap: {}", bootstrap);
        return bootstrap;
    }
}
