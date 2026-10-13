/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms;

import io.kroxylicious.testing.kms.TestKmsFacade;

/**
 * Marker for {@link TestKmsFacade}s whose proxy authenticates with the KMS as the pod's Kubernetes
 * {@code ServiceAccount}, rather than with static credentials baked into the filter configuration.
 * <p>
 * Facades implementing this interface require the proxy pods to run as a specific {@code ServiceAccount}
 * which must first be granted access to the KMS via {@link #bindToServiceAccount(String, String)}.
 */
public interface ServiceAccountAuthTestKmsFacade {
    /**
     * Grants the given ServiceAccount access to the KMS, so a proxy pod running as it can authenticate.
     *
     * @param namespace the namespace of the ServiceAccount
     * @param serviceAccountName the name of the ServiceAccount
     */
    void bindToServiceAccount(String namespace, String serviceAccountName);
}
