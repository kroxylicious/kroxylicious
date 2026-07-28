/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import io.kroxylicious.proxy.service.HostPort;

/**
 * Describes what addresses to advertise to Kafka clients in Metadata responses.
 * <p>
 * This is one of three views of a gateway's network configuration. See also
 * {@link BindingSpec} (what to bind) and {@link AddressingSpec} (how to identify connections).
 */
public interface AdvertisingSpec {

    /**
     * The full advertised bootstrap address (host and port).
     *
     * @param bootstrap the bootstrap virtual node
     * @return the bootstrap address to advertise to clients
     */
    HostPort advertisedBootstrapAddress(ProxyNodeId.Bootstrap bootstrap);

    /**
     * The full advertised broker address (host and port) for the given node.
     *
     * @param virtualNodeId the broker virtual node
     * @return the broker address to advertise to clients for this node
     * @throws IllegalArgumentException if no address can be produced for the given node
     */
    HostPort advertisedBrokerAddress(ProxyNodeId virtualNodeId) throws IllegalArgumentException;
}
