/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.vault;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultEdek;
import io.kroxylicious.kms.provider.hashicorp.vault.WrappingKey;
import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.testing.kms.TestKmsFacadeFactory;

/**
 * Abstract factory for {@link AbstractVaultTestKmsFacade}s.
 */
public abstract class AbstractVaultTestKmsFacadeFactory implements TestKmsFacadeFactory<Config, WrappingKey, VaultEdek> {
    /**
     * Creates the factory.
     */
    protected AbstractVaultTestKmsFacadeFactory() {
        // Intentionally empty
    }

    @Override
    public abstract AbstractVaultTestKmsFacade build();
}