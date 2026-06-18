/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

/**
 * Describes what hostnames to advertise to Kafka clients in Metadata responses.
 * <p>
 * This interface is intentionally host-only: it has no knowledge of ports. The port
 * component of an advertised address always comes from {@link EndpointRegistry#resolvePort(VirtualNodeId)},
 * which returns the actual port the OS bound (resolving any port=0 configuration).
 * Keeping port out of this interface eliminates port=0 as a sentinel value in the
 * advertising path.
 * <p>
 * The caller ({@code VirtualClusterGatewayModel}) assembles the full {@code HostPort} by
 * combining the host from this spec with the port from the registry:
 * <pre>
 *     String host = spec.getAdvertisedBrokerHost(vn);
 *     int    port = registry.resolvePort(vn);
 *     return new HostPort(host, port);
 * </pre>
 * <p>
 * This is one of three views of a gateway's network configuration. See also
 * {@link BindingSpec} (what to bind) and {@link RoutingSpec} (how to identify connections).
 */
public interface AdvertisingSpec {

    /**
     * Hostname to advertise for the bootstrap address.
     * The actual port is resolved separately from the registry.
     */
    String getAdvertisedBootstrapHost();

    /**
     * Hostname to advertise for the given broker node.
     * The actual port is resolved separately from the registry.
     * <p>
     * Uses {@code int nodeId} (the Kafka wire concept) rather than {@link VirtualNodeId}
     * because strategies work in Kafka's addressing space; the caller enriches with
     * gateway context to produce a {@link VirtualNodeId} for port resolution.
     *
     * @param nodeId the Kafka node id whose hostname is needed
     * @return hostname (not a full host:port) to put in Metadata responses for this node
     * @throws IllegalArgumentException if no hostname can be produced for the given nodeId
     */
    String getAdvertisedBrokerHost(int nodeId) throws IllegalArgumentException;
}
