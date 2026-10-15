/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms;

import io.kroxylicious.testing.kms.TestKmsFacade;

/**
 * Extension of {@link TestKmsFacade} for facades whose proxy authenticates with the KMS as the pod's
 * Kubernetes {@code ServiceAccount}, rather than with static credentials baked into the filter configuration.
 * <p>
 * Facades implementing this interface require the proxy pods to run as a specific {@code ServiceAccount}
 * which must first be granted access to the KMS via {@link #bindToServiceAccount(String, String)}.
 *
 * @param <C> the KMS service configuration type
 * @param <K> the key reference type
 * @param <E> the encrypted DEK type
 */
public interface ServiceAccountAuthTestKmsFacade<C, K, E> extends TestKmsFacade<C, K, E> {
    /**
     * Grants the given ServiceAccount access to the KMS, so a proxy pod running as it can authenticate.
     *
     * @param namespace the namespace of the ServiceAccount
     * @param serviceAccountName the name of the ServiceAccount
     */
    void bindToServiceAccount(String namespace, String serviceAccountName);
}
