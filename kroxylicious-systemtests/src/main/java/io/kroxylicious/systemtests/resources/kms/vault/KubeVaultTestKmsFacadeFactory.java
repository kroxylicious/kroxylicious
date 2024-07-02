/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.vault;

import io.kroxylicious.kms.provider.hashicorp.vault.AbstractVaultTestKmsFacadeFactory;

/**
 * Factory for {@link KubeVaultTestKmsFacade}s.
 */
public class KubeVaultTestKmsFacadeFactory extends AbstractVaultTestKmsFacadeFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public KubeVaultTestKmsFacade build() {
        return new KubeVaultTestKmsFacade();
    }
}
