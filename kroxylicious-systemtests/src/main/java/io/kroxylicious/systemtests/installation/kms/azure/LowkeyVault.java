/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.azure;

import java.net.URI;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Secret;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

import io.kroxylicious.kms.service.TestKmsFacadeException;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kms.azure.LowkeyVaultTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousSecretTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;
import io.kroxylicious.testing.kafka.common.KeystoreManager;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Lowkey Vault.
 */
public class LowkeyVault implements AzureKmsClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(LowkeyVault.class);
    private static final String LOWKEY_VAULT_DEPLOYMENT_NAME = "my-key-vault";
    private static final String LOWKEY_VAULT_NODE_PORT_SERVICE_NAME = "lowkey-vault-" + Constants.NODE_PORT_TYPE.toLowerCase();
    private static final String LOWKEY_VAULT_CLUSTER_IP_SERVICE_NAME = "lowkey-vault-" + Constants.CLUSTER_IP_TYPE.toLowerCase();
    private static final String LOWKEY_VAULT_DEFAULT_NAMESPACE = "lowkey-vault";
    private static final String LOWKEY_VAULT_IMAGE = Constants.DOCKER_REGISTRY_GCR_MIRROR + "/nagyesta/lowkey-vault:5.0.14";
    private final String deploymentNamespace;

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

        try {
            installCertificates();
        }
        catch (Exception e) {
            throw new TestKmsFacadeException(e);
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
        deleteCertificates();
        String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();
        NamespaceUtils.deleteNamespaceWithWaitAndRemoveFromSet(deploymentNamespace, testSuiteName);
    }

    @Override
    public URI getDefaultVaultBaseUrl() {
        return URI.create("https://" + this.getEndpointAuthority());
    }

    @Override
    public String getEndpointAuthority() {
        return DeploymentUtils.getNodePortServiceAddress(deploymentNamespace, LOWKEY_VAULT_NODE_PORT_SERVICE_NAME, 8443);
    }

    public String getDefaultNamespace() {
        return LOWKEY_VAULT_DEFAULT_NAMESPACE;
    }

    public String getLowkeyVaultClusterIpServiceName() {
        return LOWKEY_VAULT_CLUSTER_IP_SERVICE_NAME;
    }

    private void installCertificates() throws Exception {
        Path keystorePath;
        String password;
        try {
            KeystoreManager entraCertGen = new KeystoreManager();
            String domain = LOWKEY_VAULT_CLUSTER_IP_SERVICE_NAME + "." + LOWKEY_VAULT_DEFAULT_NAMESPACE + ".svc.cluster.local";
            String ipAddress = DeploymentUtils.getNodeIP();
            CertificateBuilder certificateBuilder = entraCertGen.newCertificateBuilder(entraCertGen.buildDistinguishedName("test@kroxylicious.io", domain, "Engineering",
                            "Kroxylicious.io", null, null, "US"))
                    .addSanIpAddress(ipAddress)
                    .addSanDnsName(domain);
            X509Bundle bundle = entraCertGen.createSelfSignedCertificate(certificateBuilder);
            keystorePath = entraCertGen.generateCertificateFile(bundle);
            password = entraCertGen.getPassword(keystorePath);
        }
        catch (Exception e) {
            throw new TestKmsFacadeException(e);
        }

        String defaultNamespace = KubeClusterResource.getInstance().defaultNamespace();
        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                KroxyliciousSecretTemplates.createCertificateSecret(Constants.KEYSTORE_SECRET_NAME, defaultNamespace, Constants.KEYSTORE_FILE_NAME,
                        keystorePath.toAbsolutePath().toString(), password));
        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                KroxyliciousSecretTemplates.createCertificateSecret(Constants.TRUSTSTORE_SECRET_NAME, defaultNamespace, Constants.TRUSTSTORE_FILE_NAME,
                        keystorePath.toAbsolutePath().toString(), password));
    }

    private void deleteCertificates() {
        String defaultNamespace = KubeClusterResource.getInstance().defaultNamespace();
        Secret keystore = kubeClient().getClient().secrets().inNamespace(defaultNamespace).withName(Constants.KEYSTORE_SECRET_NAME).get();
        Secret truststore = kubeClient().getClient().secrets().inNamespace(defaultNamespace).withName(Constants.TRUSTSTORE_SECRET_NAME).get();

        kubeClient().getClient().secrets().inNamespace(defaultNamespace).resource(keystore).withGracePeriod(0).delete();
        kubeClient().getClient().secrets().inNamespace(defaultNamespace).resource(truststore).withGracePeriod(0).delete();
    }
}
