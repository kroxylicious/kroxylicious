/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.vault;

import io.kroxylicious.testing.kms.vault.AbstractVaultTestKmsFacadeFactory;

/**
 * Factory for {@link KubeVaultKubernetesAuthTestKmsFacade}s.
 */
public class KubeVaultKubernetesAuthTestKmsFacadeFactory extends AbstractVaultTestKmsFacadeFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public KubeVaultKubernetesAuthTestKmsFacade build() {
        return new KubeVaultKubernetesAuthTestKmsFacade();
    }
}
