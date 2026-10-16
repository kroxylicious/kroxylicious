/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms;

import io.kroxylicious.testing.kms.TestKmsFacadeFactory;

/**
 * Marker for {@link InClusterTestKmsFacadeFactory}s whose proxy authenticates with the KMS as the pod's
 * Kubernetes {@code ServiceAccount}. These are the facades exercised by {@code ServiceAccountAuthRecordEncryptionST}.
 *
 * @param <C> the KMS service configuration type
 * @param <K> the key reference type
 * @param <E> the encrypted DEK type
 */
public interface ServiceAccountAuthKmsFacadeFactory<C, K, E> extends TestKmsFacadeFactory<C, K, E>, InClusterTestKmsFacadeFactory {
    /**
     * {@inheritDoc}
     * <p>
     * Narrowed so callers get the {@link ServiceAccountAuthTestKmsFacade} typed facade without casting.
     */
    @Override
    ServiceAccountAuthTestKmsFacade<C, K, E> build();
}
