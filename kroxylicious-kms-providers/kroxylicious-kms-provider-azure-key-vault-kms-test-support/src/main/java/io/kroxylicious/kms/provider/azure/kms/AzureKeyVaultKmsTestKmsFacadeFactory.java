/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.kms;

import io.kroxylicious.kms.provider.azure.AzureKeyVaultEdek;
import io.kroxylicious.kms.provider.azure.WrappingKey;
import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.TestKmsFacadeFactory;

/**
 * Factory for {@link AzureKeyVaultKmsTestKmsFacade}s.
 */
public class AzureKeyVaultKmsTestKmsFacadeFactory implements TestKmsFacadeFactory<AzureKeyVaultConfig, WrappingKey, AzureKeyVaultEdek> {

    @Override
    public TestKmsFacade<AzureKeyVaultConfig, WrappingKey, AzureKeyVaultEdek> build() {
        return new AzureKeyVaultKmsTestKmsFacade();
    }
}
