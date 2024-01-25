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
     * Deploy.
     *
     */
    public void deploy() {
        LOGGER.info("Deploy HashiCorp Vault in {} namespace", deploymentNamespace);
        if (kubeClient().getDeployment(deploymentNamespace, Constants.VAULT_SERVICE_NAME) != null) {
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

        ResourceManager.helmClient().namespace(deploymentNamespace).addRepository(Constants.VAULT_HELM_REPOSITORY_NAME, Constants.VAULT_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(Constants.VAULT_HELM_CHART_NAME, Constants.VAULT_SERVICE_NAME, "latest", values);

        String podName = Constants.VAULT_SERVICE_NAME + "-0";
        DeploymentUtils.waitForDeploymentRunning(deploymentNamespace, podName, Duration.ofMinutes(1));

        configureVault(deploymentNamespace, podName);
    }

    private void configureVault(String deploymentNamespace, String podName) {
        LOGGER.info("Enabling transit in vault instance");
        String loginCommand = VAULT_CMD + " login " + Constants.VAULT_ROOT_TOKEN;
        String transitCommand = VAULT_CMD + " secrets enable transit";

        int exitCode = kubeClient().getClient().pods()
                .inNamespace(deploymentNamespace)
                .withName(podName)
                .exec("sh", "-c", String.format("%s && %s", loginCommand, transitCommand)).exitCode().join();

        if (exitCode != 0) {
            throw new KubeClusterException("Cannot enable transit in vault instance!");
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
}
