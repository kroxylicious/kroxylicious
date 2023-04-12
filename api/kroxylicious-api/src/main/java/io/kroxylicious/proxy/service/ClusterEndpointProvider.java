/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import java.util.Optional;

public interface ClusterEndpointProvider {

    /** Address of the cluster's bootstrap address, includes the port.
     *
     * @return cluster's bootstrap address in the form host:port
     */
    String getClusterBootstrapAddress();

    /** Address of broker with the given node id, includes the port. Note that
     * {@code nodeId} are generally expected to be consecutively numbered and starting from zero. However gaps in the sequence can potentially emerge as the the target cluster topology evolves. 
     *
     * @return broker address in the form host:port
     * @throws IllegalArgumentException if this provider cannot produce a broker address for the given nodeId.
     */
    String getBrokerAddress(int nodeId) throws IllegalArgumentException;

    /**
     * Provides the number of broker endpoints to pre-bind.
     * Kroxylicious will pre-bind all the broker end-points between
     * 0..{@code numberOfWarmStartBrokerEndpoints}-1 on startup, rather
     * than waiting for the first bootstrap connection to allow their
     * discovery.
     *
     * @return number of endpoints to pre-bind on startup.
     */
    default int getNumberOfBrokerEndpointsToPrebind() {
        return 1;
    }

    /**
     * Gets the bind address used when binding socket.  Used to restrict
     * listening to particular network interfaces.
     * @return bind address such as "127.0.0.1" or {@code }Optional.empty()} if all address should be bound.
     */
    default Optional<String> getBindAddress() {
        return Optional.empty();
    }

}
