/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

/**
 * Factory for {@link VaultTestKmsFacade}s.
 */
public class VaultTestKmsFacadeFactory extends AbstractVaultTestKmsFacadeFactory {
    /**
     * {@inheritDoc}
     */
    @Override
    public AbstractVaultTestKmsFacade build() {
        return new VaultTestKmsFacade();
    }
}
