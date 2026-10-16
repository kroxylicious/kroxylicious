/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.vault;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultEdek;
import io.kroxylicious.kms.provider.hashicorp.vault.WrappingKey;
import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.systemtests.resources.kms.ServiceAccountAuthKmsFacadeFactory;
import io.kroxylicious.testing.kms.vault.AbstractVaultTestKmsFacadeFactory;

/**
 * Factory for {@link KubeVaultKubernetesAuthTestKmsFacade}s.
 */
public class KubeVaultKubernetesAuthTestKmsFacadeFactory extends AbstractVaultTestKmsFacadeFactory
        implements ServiceAccountAuthKmsFacadeFactory<Config, WrappingKey, VaultEdek> {

    /**
     * {@inheritDoc}
     */
    @Override
    public KubeVaultKubernetesAuthTestKmsFacade build() {
        return new KubeVaultKubernetesAuthTestKmsFacade();
    }
}
