/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsService.Config;
import io.kroxylicious.kms.service.TestKmsFacadeFactory;

/**
 * Factory for {@link VaultKmsFacade}s.
 */
public class VaultKmsFacadeFactory implements TestKmsFacadeFactory<Config, String, VaultEdek> {
    /**
     * {@inheritDoc}
     */
    @Override
    public VaultKmsFacade build() {
        return new VaultKmsFacade();
    }
}
