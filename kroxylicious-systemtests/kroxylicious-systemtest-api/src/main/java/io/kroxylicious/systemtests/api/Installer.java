/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api;

/**
 * Handles getting project infrastructure components (operator, webhook, CRDs, RBAC)
 * into the cluster.
 * <p>
 * This is the primary extension point for downstream distributors, who typically vary only
 * by installation method (their own OLM catalog, their own Helm chart) and not by how
 * proxies are deployed. A downstream distributor implements {@code Installer} (or configures
 * an upstream-provided one) and composes it with upstream's fixtures.
 * <p>
 * The implementation is selected via the {@code KROXYLICIOUS_TEST_INSTALLER} environment variable.
 */
public interface Installer {

    /**
     * Installs the given component into the cluster. Blocks until the component is ready.
     *
     * @param component the component to install
     */
    void install(Component component);

    /**
     * Uninstalls the given component from the cluster.
     *
     * @param component the component to uninstall
     */
    void uninstall(Component component);
}
