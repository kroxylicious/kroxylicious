/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.vault;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultEdek;
import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.testing.kms.TestKmsFacadeFactory;

public abstract class AbstractVaultTestKmsFacadeFactory implements TestKmsFacadeFactory<Config, String, VaultEdek> {
    @Override
    public abstract AbstractVaultTestKmsFacade build();
}