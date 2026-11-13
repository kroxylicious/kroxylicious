/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api;

/**
 * A token of convergence — proof that a proxy is running and serving the requested configuration.
 * Obtained exclusively from {@link ProxyFixture#apply(ProxyDefinition)}.
 * <p>
 * A test cannot interact with the proxy before convergence has been waited for,
 * because the only way to obtain a {@code ProxyHandle} is through a blocking {@code apply()} call.
 */
public interface ProxyHandle {

    /**
     * Returns the bootstrap server address for an on-cluster client.
     * Equivalent to {@code bootstrap(ClientLocation.ON_CLUSTER)}.
     *
     * @return bootstrap address (host:port)
     */
    default String bootstrap() {
        return bootstrap(ClientLocation.ON_CLUSTER);
    }

    /**
     * Returns the bootstrap server address appropriate for the given client location.
     * Off-cluster clients receive an externally accessible address; on-cluster clients
     * receive the cluster-internal service address.
     *
     * @param location where the client runs
     * @return bootstrap address (host:port)
     */
    String bootstrap(ClientLocation location);

    /**
     * Reconfigures the proxy to serve a new definition and blocks until convergence.
     * The old handle should not be used after calling this method.
     *
     * @param definition the new proxy definition
     * @return a new handle for the reconfigured proxy
     */
    ProxyHandle reconfigure(ProxyDefinition definition);

    /**
     * Blocks until the proxy has restarted and is serving again.
     * Use this when the test triggers conditions that cause a restart
     * (e.g. certificate rotation, config change) and needs to wait for recovery.
     */
    void waitForRestart();
}
