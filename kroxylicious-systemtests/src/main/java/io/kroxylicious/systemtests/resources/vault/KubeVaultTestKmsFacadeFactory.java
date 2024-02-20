/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.vault;

import io.kroxylicious.kms.provider.hashicorp.vault.AbstractVaultTestKmsFacade;
import io.kroxylicious.kms.provider.hashicorp.vault.AbstractVaultTestKmsFacadeFactory;

/**
 * Factory for {@link KubeVaultTestKmsFacade}s.
 */
public class KubeVaultTestKmsFacadeFactory extends AbstractVaultTestKmsFacadeFactory {

    /**
     * Build kube vault test kms facade.
     *
     * @param namespace the namespace
     * @param podName the pod name
     * @param isOpenshiftCluster the boolean for openshift cluster
     * @return the kube vault test kms facade
     */
    public KubeVaultTestKmsFacade build(String namespace, String podName, boolean isOpenshiftCluster) {
        return new KubeVaultTestKmsFacade(namespace, podName, isOpenshiftCluster);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AbstractVaultTestKmsFacade build() {
        throw new UnsupportedOperationException();
    }
}
