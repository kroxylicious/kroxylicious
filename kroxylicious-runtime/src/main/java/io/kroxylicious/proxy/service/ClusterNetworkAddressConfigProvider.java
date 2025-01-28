/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Provides the addresses of the network endpoints required by a virtual cluster.
 */
public interface ClusterNetworkAddressConfigProvider {

    /**
     * Address of the cluster's bootstrap address.
     *
     * @return cluster's bootstrap address.
     */
    HostPort getClusterBootstrapAddress();

    /**
     * Address of broker with the given node id, (advertised hostname and bind port). Note that
     * {@code nodeId} are generally expected to be consecutively numbered and starting from zero. However,
     * gaps in the sequence can potentially emerge as the target cluster topology evolves.
     *
     * @param nodeId node identifier
     * @return broker address
     * @throws IllegalArgumentException if this provider cannot produce a broker address for the given nodeId.
     */
    HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException;

    /**
     * Advertised address of broker with the given node id, (advertised hostname and advertised port). This is
     * what is returned to clients and may differ from the node's bind port as presented by {@link #getBrokerAddress(int)}.
     * This enables Kroxylicious to sit behind yet another proxy that uses a different port from the kroxylicious bind port.
     * @param nodeId node id
     * @return the port to advertise for the nodeId
     * @throws IllegalArgumentException if this provider cannot produce a broker address for the given nodeId.
     */
    default HostPort getAdvertisedBrokerAddress(int nodeId) throws IllegalArgumentException {
        return getBrokerAddress(nodeId);
    }

    /**
     * Generates the node id implied by the given broker address (advertised hostname and bind port).
     * This method make sense only for implementation that embed node id information into the broker
     * address.  This information is used at startup time to allow a client that already in possession
     * of a broker address to reconnect to the cluster via Kroxylicious using only that address.
     * <br/>
     * This is an optional method. An implementation can return null.
     *
     * @param brokerAddress broker address
     * @return a broker id or null if the broker id cannot be
     */
    default Integer getBrokerIdFromBrokerAddress(HostPort brokerAddress) {
        return null;
    }

    /**
     * Gets the bind address used when binding socket.  Used to restrict
     * listening to particular network interfaces.
     * @return bind address such as "127.0.0.1" or {@code }Optional.empty()} if all address should be bound.
     */
    default Optional<String> getBindAddress() {
        return Optional.empty();
    }

    /**
     * Indicates if this provider requires the use of TLS.
     *
     * @return true if this provider requires the use of TLS.
     */
    default boolean requiresTls() {
        return false;
    }

    /**
     * Set of ports number that this provider requires exclusive use.
     *
     * @return set of port numbers
     */
    default Set<Integer> getExclusivePorts() {
        return Set.of();
    }

    /**
     * Set of ports number that this provider can share with another provider.
     *
     * @return set of port numbers
     */
    default Set<Integer> getSharedPorts() {
        return Set.of();
    }

    /**
     * Map of discovery addresses that will be bound by the virtual cluster on start-up
     * (before the first reconcile has taken place).  Discovery bindings always
     * point to a target cluster's bootstrap and are used for purposes of metadata
     * discovery only.
     * <br/>
     * Discovery bindings are replaced by bindings to upstream brokers on reconciliation.
     *
     * @return discovery address map
     */
    default Map<Integer, HostPort> discoveryAddressMap() {
        return Map.of();
    }

}
