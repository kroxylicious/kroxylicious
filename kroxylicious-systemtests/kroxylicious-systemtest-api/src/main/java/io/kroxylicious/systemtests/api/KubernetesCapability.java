/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * Provides access to the Kubernetes environment the proxy was deployed into.
 * Available for any Kubernetes-backed fixture (CRD, manifest, sidecar) — not available
 * for standalone fixtures.
 * <p>
 * Injected as a test parameter by the JUnit extension when the test runs against
 * a Kubernetes deployment.
 */
public interface KubernetesCapability {

    /**
     * Returns the namespace the proxy was deployed into.
     *
     * @return Kubernetes namespace
     */
    String namespace();

    /**
     * Returns a Kubernetes client configured for the cluster the proxy runs in.
     *
     * @return Kubernetes client
     */
    KubernetesClient client();
}
