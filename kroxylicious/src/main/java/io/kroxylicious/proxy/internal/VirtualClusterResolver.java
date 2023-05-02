/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import io.kroxylicious.proxy.config.VirtualCluster;

/**
 * Used by the {@link io.kroxylicious.proxy.internal.KafkaProxyInitializer} to resolve host/port information
 * provided by the Netty {@link java.nio.channels.Channel} to a {@link VirtualCluster}.
 */
public interface VirtualClusterResolver {
    /**
     * Uses host/port information to resolve a {@link VirtualCluster}.  If exactly one virtual cluster cannot be
     * identified, a {@link VirtualClusterResolutionException} will be thrown.
     *
     * @param sniHostname SNI hostname provided by the TLS client-hello
     * @param targetPort target port of the connection.
     * @return virtual cluster
     * @throws VirtualClusterResolutionException there was no exact match found for the given host/port combination.
     */
    VirtualCluster resolve(String sniHostname, int targetPort) throws VirtualClusterResolutionException;

    /**
     * Uses port information to resolve a {@link VirtualCluster}.  If exactly one virtual cluster cannot be
     * identified, a {@link VirtualClusterResolutionException} will be thrown.
     *
     * @param targetPort target port of the connection.
     * @return virtual cluster
     * @throws VirtualClusterResolutionException there was no exact match found for the given port combination.
     */
    VirtualCluster resolve(int targetPort) throws VirtualClusterResolutionException;
}
