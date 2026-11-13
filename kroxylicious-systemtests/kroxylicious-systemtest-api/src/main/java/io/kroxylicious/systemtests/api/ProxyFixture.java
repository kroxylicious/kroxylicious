/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api;

/**
 * Translates a {@link ProxyDefinition} into running infrastructure, blocks until the proxy
 * has converged, and returns a {@link ProxyHandle}.
 * <p>
 * Implementations correspond to deployment mechanisms: CRD-based (operator-managed),
 * manifest-based, sidecar, or standalone. Feature tests are portable across all implementations —
 * the same test calls {@code apply(definition)} and gets a {@code ProxyHandle} regardless
 * of how the proxy was deployed.
 * <p>
 * The implementation is selected via the {@code KROXYLICIOUS_TEST_FIXTURE} environment variable
 * and injected by the JUnit extension at class scope.
 */
public interface ProxyFixture {

    /**
     * Deploys a proxy matching the given definition and blocks until it has converged.
     * When this method returns, the proxy is serving the requested configuration
     * to the best observable precision.
     *
     * @param definition the desired proxy configuration
     * @return a handle to the converged proxy
     */
    ProxyHandle apply(ProxyDefinition definition);
}
