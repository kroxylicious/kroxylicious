/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

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
     * Address of broker with the given node id, includes the port. Note that
     * {@code nodeId} are generally expected to be consecutively numbered and starting from zero. However, gaps in the sequence can potentially emerge as
     * the target cluster topology evolves.
     *
     * @param nodeId node identifier
     * @return broker address
     * @throws IllegalArgumentException if this provider cannot produce a broker address for the given nodeId.
     */
    HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException;

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

    Set<Integer> prebindBrokerIds();
}
