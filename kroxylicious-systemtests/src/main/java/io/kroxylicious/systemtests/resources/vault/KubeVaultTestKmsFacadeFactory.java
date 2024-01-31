/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.vault;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.TestKmsFacadeFactory;

/**
 * Factory for {@link KubeVaultTestKmsFacade}s.
 */
public class KubeVaultTestKmsFacadeFactory implements TestKmsFacadeFactory<Config, String, VaultEdek> {

    public KubeVaultTestKmsFacade build(String namespace, String podName) {
        return new KubeVaultTestKmsFacade(namespace, podName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TestKmsFacade<Config, String, VaultEdek> build() {
        throw new UnsupportedOperationException();
    }
}
