/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.azure;

import java.net.URI;

import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.provider.azure.config.auth.Oauth2ClientCredentialsConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.installation.kms.azure.AzureKeyVaultCloud;
import io.kroxylicious.testing.kms.TestKekManager;
import io.kroxylicious.testing.kms.azure.AbstractAzureKeyVaultKmsTestKmsFacade;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * KMS Facade backed by a real Azure Key Vault. Active only when {@code USE_CLOUD_KMS=true}
 * and the {@code AZURE_*} credentials are supplied. Mirror of
 * {@link io.kroxylicious.systemtests.resources.kms.aws.KubeAwsKmsCloudTestKmsFacade}.
 */
public class KubeAzureKeyVaultCloudTestKmsFacade extends AbstractAzureKeyVaultKmsTestKmsFacade {

    private final AzureKeyVaultCloud azureKeyVaultCloud = new AzureKeyVaultCloud();

    /**
     * Instantiates a new Kube Azure Key Vault cloud test kms facade.
     */
    public KubeAzureKeyVaultCloudTestKmsFacade() {
    }

    @Override
    public boolean isAvailable() {
        return azureKeyVaultCloud.isAvailable();
    }

    @Override
    public void startKms() {
        azureKeyVaultCloud.deploy();
    }

    @Override
    public void stopKms() {
        azureKeyVaultCloud.delete();
    }

    @NonNull
    @Override
    public AzureKeyVaultConfig getKmsServiceConfig() {
        return new AzureKeyVaultConfig(
                buildOauthConfig(),
                null,
                Environment.AZURE_KEY_VAULT_NAME,
                Constants.AZURE_KEY_VAULT_HOST,
                null,
                null,
                null);
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new AzureKmsTestKekManager(
                Environment.AZURE_TENANT_ID,
                Environment.AZURE_CLIENT_ID,
                Environment.AZURE_CLIENT_SECRET,
                azureKeyVaultCloud.getDefaultVaultBaseUrl().toString());
    }

    private static Oauth2ClientCredentialsConfig buildOauthConfig() {
        return new Oauth2ClientCredentialsConfig(
                URI.create(Constants.AZURE_OAUTH_ENDPOINT),
                Environment.AZURE_TENANT_ID,
                new InlinePassword(Environment.AZURE_CLIENT_ID),
                new InlinePassword(Environment.AZURE_CLIENT_SECRET),
                URI.create(Constants.AZURE_KEY_VAULT_SCOPE),
                null);
    }
}
