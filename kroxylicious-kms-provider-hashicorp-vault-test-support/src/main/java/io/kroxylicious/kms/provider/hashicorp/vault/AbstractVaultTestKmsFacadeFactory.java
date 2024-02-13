/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.service.TestKmsFacadeFactory;

public abstract class AbstractVaultTestKmsFacadeFactory implements TestKmsFacadeFactory<Config, String, VaultEdek> {
    @Override
    public abstract AbstractVaultTestKmsFacade build();
}