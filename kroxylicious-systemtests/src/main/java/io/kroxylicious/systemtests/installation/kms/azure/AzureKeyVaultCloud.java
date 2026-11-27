/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.azure;

import java.net.URI;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;

/**
 * {@link AzureKmsClient} for a real Azure Key Vault. There is nothing to deploy; the vault,
 * the service principal and the encryption keys are provisioned out of band and supplied
 * through {@code AZURE_*} environment variables. Mirror of
 * {@link io.kroxylicious.systemtests.installation.kms.aws.AwsKmsCloud}.
 */
public class AzureKeyVaultCloud implements AzureKmsClient {

    /**
     * Instantiates a new Azure Key Vault cloud client.
     */
    public AzureKeyVaultCloud() {
    }

    @Override
    public boolean isAvailable() {
        return isNotBlank(Environment.AZURE_TENANT_ID)
                && isNotBlank(Environment.AZURE_CLIENT_ID)
                && isNotBlank(Environment.AZURE_CLIENT_SECRET)
                && Environment.USE_CLOUD_KMS.equalsIgnoreCase("true");
    }

    @Override
    public void deploy() {
        // real cloud service, nothing to deploy
    }

    @Override
    public void delete() {
        // nothing to delete
    }

    @Override
    public URI getDefaultVaultBaseUrl() {
        return URI.create("https://" + getEndpointAuthority());
    }

    @Override
    public String getEndpointAuthority() {
        return Environment.AZURE_KEY_VAULT_NAME + "." + Constants.AZURE_KEY_VAULT_HOST;
    }

    private static boolean isNotBlank(String value) {
        return value != null && !value.isBlank();
    }
}
