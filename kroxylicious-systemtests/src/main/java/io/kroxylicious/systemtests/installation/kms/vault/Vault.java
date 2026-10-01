/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.vault;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.openshift.api.model.operator.v1.IngressControllerList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.skodjob.kubetest4j.utils.KubeUtils;

import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;
import io.kroxylicious.systemtests.utils.TestUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

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
    private static final Pattern PORT_FORWARD_ADDRESS = Pattern.compile("Forwarding from 127\\.0\\.0\\.1:(\\d+)");
    private final String deploymentNamespace;
    private final String vaultRootToken;
    private Process portForwardProcess;
    private Thread portForwardOutputThread;
    private URI vaultUrl;

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

        boolean openshiftCluster = KubeUtils.isOcp();
        LOGGER.info("Deploy HashiCorp Vault in {} namespace, openshift: {}", deploymentNamespace, openshiftCluster);

        NamespaceUtils.createNamespaceAndPrepare(deploymentNamespace);
        ResourceManager.helmClient().addRepository(VAULT_HELM_REPOSITORY_NAME, VAULT_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(VAULT_HELM_CHART_NAME, VAULT_SERVICE_NAME,
                Optional.empty(),
                Optional.of(Path.of(TestUtils.getResourcesURI("helm_vault_overrides.yaml"))),
                Optional.of(Map.of("server.dev.devRootToken", vaultRootToken,
                        "global.openshift", String.valueOf(openshiftCluster),
                        "server.route.enabled", String.valueOf(openshiftCluster),
                        "server.route.host", VAULT_SERVICE_NAME + "." + getIngressDomain(openshiftCluster),
                        "server.route.tls", "null")));
        if (!openshiftCluster) {
            startPortForward();
        }
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
        stopPortForward();
        String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();
        NamespaceUtils.deleteNamespaceWithWaitAndRemoveFromSet(deploymentNamespace, testSuiteName);
    }

    /**
     * Gets the vault url (accessible from the host test runner).
     *
     * @return the vault url.
     */
    public URI getVaultUrl() {
        if (KubeUtils.isOcp()) {
            return URI.create("http://" + DeploymentUtils.getOpenshiftRouteServiceAddress(deploymentNamespace, VAULT_SERVICE_NAME));
        }
        else {
            return vaultUrl;
        }
    }

    /**
     * Gets the in-cluster vault url (accessible from pods inside Kubernetes).
     *
     * @return the in-cluster vault url.
     */
    public URI getInClusterVaultUrl() {
        return URI.create("http://" + VAULT_SERVICE_NAME + "." + deploymentNamespace + ".svc:8200");
    }

    private void startPortForward() {
        await().atMost(Duration.ofMinutes(2)).until(() -> kubeClient().getClient().endpoints()
                .inNamespace(deploymentNamespace)
                .withName(VAULT_SERVICE_NAME)
                .get() != null);
        try {
            portForwardProcess = new ProcessBuilder("kubectl", "port-forward", "--namespace", deploymentNamespace,
                    "service/" + VAULT_SERVICE_NAME, "0:8200")
                    .redirectErrorStream(true)
                    .start();
            BufferedReader output = new BufferedReader(new InputStreamReader(portForwardProcess.getInputStream()));
            String line = output.readLine();
            if (line == null) {
                throw new IllegalStateException("kubectl port-forward exited without producing output");
            }
            Matcher matcher = PORT_FORWARD_ADDRESS.matcher(line);
            if (!matcher.find()) {
                throw new IllegalStateException("Unexpected kubectl port-forward output: " + line);
            }
            vaultUrl = URI.create("http://127.0.0.1:" + matcher.group(1));
            LOGGER.info("Forwarding Vault service to {}", vaultUrl);
            portForwardOutputThread = new Thread(() -> drainPortForwardOutput(output), "vault-port-forward-output");
            portForwardOutputThread.setDaemon(true);
            portForwardOutputThread.start();
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to start kubectl port-forward for Vault", e);
        }
    }

    private void drainPortForwardOutput(BufferedReader output) {
        try (output) {
            while (output.readLine() != null) {
                // Keep the port-forward process output pipe drained.
            }
        }
        catch (IOException e) {
            LOGGER.debug("Vault port-forward output stream closed", e);
        }
    }

    private void stopPortForward() {
        if (portForwardProcess != null) {
            portForwardProcess.destroy();
            portForwardProcess = null;
        }
        portForwardOutputThread = null;
    }
}
