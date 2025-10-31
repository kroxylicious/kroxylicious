/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.azure;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kms.azure.LowkeyVaultTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Lowkey Vault.
 */
public class LowkeyVault implements AzureKmsClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(LowkeyVault.class);
    public static final String LOWKEY_VAULT_DEPLOYMENT_NAME = "my-key-vault";
    public static final String LOWKEY_VAULT_NODE_PORT_SERVICE_NAME = "lowkey-vault-" + Constants.NODE_PORT_TYPE.toLowerCase();
    public static final String LOWKEY_VAULT_CLUSTER_IP_SERVICE_NAME = "lowkey-vault-" + Constants.CLUSTER_IP_TYPE.toLowerCase();
    private static final String LOWKEY_VAULT_DEFAULT_NAMESPACE = "lowkey-vault";
    private static final String LOWKEY_VAULT_IMAGE = Constants.DOCKER_REGISTRY_GCR_MIRROR + "/nagyesta/lowkey-vault:5.0.0";
    private final String deploymentNamespace;
    // private final HttpClient httpClient = HttpClient.newHttpClient();

    /**
     * Instantiates a new Lowkey Vault.
     *
     */
    public LowkeyVault() {
        this.deploymentNamespace = LOWKEY_VAULT_DEFAULT_NAMESPACE;
    }

    @Override
    public boolean isAvailable() {
        return !Environment.KMS_USE_CLOUD.equalsIgnoreCase("true");
    }

    private boolean isDeployed() {
        return kubeClient().getService(deploymentNamespace, LOWKEY_VAULT_NODE_PORT_SERVICE_NAME) != null;
    }

    @Override
    public void deploy() {
        if (isDeployed()) {
            LOGGER.warn("Skipping LowKey Vault deployment. It is already deployed!");
            return;
        }

        LOGGER.info("Deploy LowKey Vault in {} namespace", deploymentNamespace);
        NamespaceUtils.createNamespaceAndPrepare(deploymentNamespace);
        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                LowkeyVaultTemplates.defaultLowkeyVaultNodePortService(LOWKEY_VAULT_NODE_PORT_SERVICE_NAME, deploymentNamespace, LOWKEY_VAULT_DEPLOYMENT_NAME));
        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                LowkeyVaultTemplates.defaultLowkeyVaultClusterIPService(LOWKEY_VAULT_CLUSTER_IP_SERVICE_NAME, deploymentNamespace, LOWKEY_VAULT_DEPLOYMENT_NAME));

        DeploymentUtils.copySecretInNamespace(deploymentNamespace, Constants.KEYSTORE_SECRET_NAME);

        String password = DeploymentUtils.getSecretValue(deploymentNamespace, Constants.KEYSTORE_SECRET_NAME, "password");

        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                LowkeyVaultTemplates.defaultLowkeyVaultDeployment(LOWKEY_VAULT_DEPLOYMENT_NAME, LOWKEY_VAULT_IMAGE, deploymentNamespace, getEndpointAuthority(),
                        password));
    }

    @Override
    public void delete() {
        LOGGER.info("Deleting Lowkey Vault in {} namespace", deploymentNamespace);
        String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();
        NamespaceUtils.deleteNamespaceWithWaitAndRemoveFromSet(deploymentNamespace, testSuiteName);
    }

    @Override
    public URI getDefaultVaultBaseUrl() {
        return URI.create("https://" + this.getEndpointAuthority());
    }

    // public String getDefaultVaultAuthority() {
    // return DeploymentUtils.getNodePortServiceAddress(deploymentNamespace, LOWKEY_VAULT_NODE_PORT_SERVICE_NAME, 8443);
    // }

    // public KeyStore getDefaultKeyStore() {
    // HttpRequest request = HttpRequest.newBuilder().uri(URI.create(this.getTokenEndpointBaseUrl() + "/metadata/default-cert/lowkey-vault.p12")).GET().build();
    //
    // try {
    // byte[] keyStoreBytes = this.httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray()).body();
    // KeyStore keyStore = KeyStore.getInstance("PKCS12");
    // keyStore.load(new ByteArrayInputStream(keyStoreBytes), this.getDefaultKeyStorePassword().toCharArray());
    // return keyStore;
    // }
    // catch (InterruptedException e) {
    // Thread.currentThread().interrupt();
    // throw new IllegalStateException("Failed to get default key store", e);
    // }
    // catch (Exception e) {
    // throw new IllegalStateException("Failed to get default key store", e);
    // }
    // }
    //
    // public String getDefaultKeyStorePassword() {
    // HttpRequest request = HttpRequest.newBuilder().uri(URI.create(this.getTokenEndpointBaseUrl() + "/metadata/default-cert/password")).GET().build();
    //
    // try {
    // return this.httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)).body();
    // }
    // catch (InterruptedException e) {
    // Thread.currentThread().interrupt();
    // throw new IllegalStateException("Failed to get default key store password", e);
    // }
    // catch (Exception e) {
    // throw new IllegalStateException("Failed to get default key store password", e);
    // }
    // }

    // public String getTokenEndpointBaseUrl() {
    // return "http://" + DeploymentUtils.getNodePortServiceAddress(deploymentNamespace, LOWKEY_VAULT_DEPLOYMENT_NAME, 8080);
    // }

    public String getEndpointAuthority() {
        return DeploymentUtils.getNodePortServiceAddress(deploymentNamespace, LOWKEY_VAULT_NODE_PORT_SERVICE_NAME, 8443);
    }

    public String getDefaultNamespace() {
        return LOWKEY_VAULT_DEFAULT_NAMESPACE;
    }
}
