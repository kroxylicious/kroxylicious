/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.service.TestKmsFacadeFactory;

/**
 * Factory for {@link VaultTestKmsFacade}s.
 */
public class VaultTestKmsFacadeFactory extends AbstractVaultTestKmsFacadeFactory implements TestKmsFacadeFactory<Config, String, VaultEdek> {
    /**
     * {@inheritDoc}
     */
    @Override
    public AbstractVaultTestKmsFacade build() {
        return new VaultTestKmsFacade();
    }
}
