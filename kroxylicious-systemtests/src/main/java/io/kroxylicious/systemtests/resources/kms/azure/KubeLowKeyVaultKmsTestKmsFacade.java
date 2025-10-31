/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.azure;

import java.net.URI;

import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.provider.azure.config.auth.Oauth2ClientCredentialsConfig;
import io.kroxylicious.kms.provider.azure.kms.AbstractAzureKeyVaultKmsTestKmsFacade;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacadeException;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.installation.kms.azure.LowkeyVault;
import io.kroxylicious.systemtests.installation.kms.azure.MockOauthServer;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * KMS Facade for Azure Kms running inside Kube (LowKeyVault).
 */
public class KubeLowKeyVaultKmsTestKmsFacade extends AbstractAzureKeyVaultKmsTestKmsFacade {
    private final LowkeyVault lowKeyVault;
    private final MockOauthServer mockOauthServer;

    /**
     * Instantiates a new Kube LowKeyVault test kms facade.
     *
     */
    public KubeLowKeyVaultKmsTestKmsFacade() {
        this.lowKeyVault = new LowkeyVault();
        this.mockOauthServer = new MockOauthServer();
    }

    @Override
    public boolean isAvailable() {
        return lowKeyVault.isAvailable() && mockOauthServer.isAvailable();
    }

    @Override
    public void startKms() {
        lowKeyVault.deploy();
        mockOauthServer.deploy();
    }

    @Override
    public void stopKms() {
        lowKeyVault.delete();
        mockOauthServer.delete();
    }

    @NonNull
    @Override
    public AzureKeyVaultConfig getKmsServiceConfig() {
        if (!isAvailable()) {
            throw new IllegalStateException("kms is not initialized");
        }
        // URI defaultVaultBaseUrl = lowKeyVault.getDefaultVaultBaseUrl();
        try {
            String password = DeploymentUtils.getSecretValue(lowKeyVault.getDefaultNamespace(), Constants.KEYSTORE_SECRET_NAME, "password");
            String keyVaultHost = lowKeyVault.getDefaultNamespace() + ".svc.cluster.local";

            TrustStore vaultTrust = new TrustStore("${secret:" + Constants.KEYSTORE_SECRET_NAME + ":" + Constants.KEYSTORE_FILE_NAME + "}",
                    new InlinePassword(password), "JKS");
            Tls vaultTls = new Tls(null, vaultTrust, null, null);
            TrustStore entraTrust = new TrustStore("${secret:" + Constants.TRUSTSTORE_SECRET_NAME + ":" + Constants.TRUSTSTORE_FILE_NAME + "}",
                    new InlinePassword(password), "JKS");
            Tls entraTls = new Tls(null, entraTrust, null, null);
            return new AzureKeyVaultConfig(
                    new Oauth2ClientCredentialsConfig(mockOauthServer.getBaseUri(), "tenant2", new InlinePassword("abc"), new InlinePassword("def"),
                            URI.create(
                                    "https://vault.azure.net/.default")/* URI.create("https://" + LowkeyVault.LOWKEY_VAULT_CLUSTER_IP_SERVICE_NAME + "." + keyVaultHost + ":443") */,
                            entraTls),
                    null, LowkeyVault.LOWKEY_VAULT_CLUSTER_IP_SERVICE_NAME, keyVaultHost /* defaultVaultBaseUrl.getHost() */, null,
                    443/* defaultVaultBaseUrl.getPort() */, vaultTls);
        }
        catch (Exception e) {
            throw new TestKmsFacadeException(e);
        }
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new AzureKmsTestKekManager(lowKeyVault.getEndpointAuthority(), lowKeyVault.getEndpointAuthority(), lowKeyVault.getDefaultVaultBaseUrl().toString());
    }
}
