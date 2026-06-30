/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Optional;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Maps an incoming connection to the Kafka nodeId it targets.
 * <p>
 * This is asked per-connection by the dispatch layer as connections arrive on bound sockets.
 * The result identifies which virtual cluster and which broker node the client wants to
 * reach, or indicates a bootstrap connection (no specific node).
 * <p>
 * This is one of three views of a gateway's network configuration. See also
 * {@link BindingSpec} (what to bind) and {@link AdvertisingSpec} (what to advertise).
 */
public interface RoutingSpec {

    /**
     * Identifies the target of an incoming connection.
     * <p>
     * Returns the Kafka nodeId for broker-specific connections, or empty for bootstrap
     * connections. The caller wraps the nodeId with the gateway reference to construct
     * the appropriate {@link VirtualNodeId}.
     *
     * @param port        the actual local port the connection arrived on (never zero)
     * @param sniHostname the TLS SNI hostname presented by the client, or {@code null} for
     *                    plain connections or TLS connections without SNI
     * @return the Kafka nodeId for broker-specific connections, or
     *         {@link Optional#empty()} for bootstrap connections
     */
    Optional<Integer> identify(int port, @Nullable String sniHostname);
}
