/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.azure;

import io.kroxylicious.kms.provider.azure.AzureKeyVaultEdek;
import io.kroxylicious.kms.provider.azure.WrappingKey;
import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.testing.kms.TestKmsFacadeFactory;

public abstract class AbstractAzureKeyVaultKmsTestKmsFacadeFactory implements TestKmsFacadeFactory<AzureKeyVaultConfig, WrappingKey, AzureKeyVaultEdek> {
    @Override
    public abstract AbstractAzureKeyVaultKmsTestKmsFacade build();
}
