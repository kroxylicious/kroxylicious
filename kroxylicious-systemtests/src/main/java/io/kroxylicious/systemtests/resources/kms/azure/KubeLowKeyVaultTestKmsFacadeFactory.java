/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.azure;

import io.kroxylicious.kms.provider.azure.kms.AbstractAzureKeyVaultKmsTestKmsFacadeFactory;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Factory for {@link KubeLowKeyVaultKmsTestKmsFacade}s.
 */
public class KubeLowKeyVaultTestKmsFacadeFactory extends AbstractAzureKeyVaultKmsTestKmsFacadeFactory {

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public KubeLowKeyVaultKmsTestKmsFacade build() {
        return new KubeLowKeyVaultKmsTestKmsFacade();
    }
}
