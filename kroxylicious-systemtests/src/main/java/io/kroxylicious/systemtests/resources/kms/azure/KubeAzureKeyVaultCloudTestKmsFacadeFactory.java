/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.azure;

import io.kroxylicious.testing.kms.azure.AbstractAzureKeyVaultKmsTestKmsFacadeFactory;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Factory for {@link KubeAzureKeyVaultCloudTestKmsFacade}s.
 */
public class KubeAzureKeyVaultCloudTestKmsFacadeFactory extends AbstractAzureKeyVaultKmsTestKmsFacadeFactory {

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public KubeAzureKeyVaultCloudTestKmsFacade build() {
        return new KubeAzureKeyVaultCloudTestKmsFacade();
    }
}
