/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import io.netty.handler.ssl.SslContext;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

/**
 * A gateway to an endpoint.
 */
public interface EndpointGateway {
    /**
     * Target cluster associated with this listener.
     * <p>
     * Only meaningful for direct-routed virtual clusters, used by bindings that need an upstream
     * bootstrap target for metadata discovery before broker topology is known. Not used by
     * identity resolution ({@link AddressingSpec}/{@link BindingSpec}), which never needs to know
     * where a connection is ultimately forwarded to.
     *
     * @return target cluster
     * @throws UnsupportedOperationException if this gateway's virtual cluster uses dynamic routing
     */
    TargetCluster targetCluster();

    /**
     * true if this listener uses TLS.
     *
     * @return true if listener uses TLS.
     */
    boolean isUseTls();

    /**
     * Indicates if the provider requires that connections utilise the Server Name Indication (SNI)
     * extension to TLS.  If this is true, then the provider cannot support plain connections.
     *
     * @return true if this provider requires Server Name Indication (SNI).
     */
    boolean requiresServerNameIndication();

    VirtualClusterModel virtualCluster();

    /**
     * Bootstrap address.
     *
     * @return bootstrap address.
     */
    HostPort getClusterBootstrapAddress();

    /**
     * Broker address for given nodeId.
     *
     * @param nodeId node id
     * @return broker address
     * @throws IllegalArgumentException address for given broker node cannot be generated.
     */
    HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException;

    Optional<SslContext> getDownstreamSslContext();

    /**
     * Advertised address of broker with the given node id, (advertised hostname and advertised port). This is
     * what is returned to clients and may differ from the node's bind port as presented by {@link #getBrokerAddress(int)}.
     * This enables Kroxylicious to sit behind yet another proxy that uses a different port from the kroxylicious bind port.
     * @param nodeId node id
     * @return the broker's advertised address
     * @throws IllegalArgumentException if this provider cannot produce a broker address for the given nodeId.
     */
    HostPort getAdvertisedBrokerAddress(int nodeId);

    /**
     * Bind address to be used for network binds.
     *
     * @return bind address
     */
    Optional<String> getBindAddress();

    Set<Integer> getExclusivePorts();

    Set<Integer> getSharedPorts();

    /**
     * Map of node ids to broker addresses.
     *
     * @return map of addresses
     */
    Map<Integer, HostPort> discoveryAddressMap();

    /**
     * Get the gateways name
     * @return name
     */
    String name();

    /**
     * The binding specification describing what sockets this gateway needs.
     *
     * @return the binding spec
     */
    BindingSpec bindingSpec();

    /**
     * The addressing specification used to identify the target of an incoming connection.
     *
     * @return the addressing spec
     */
    AddressingSpec addressingSpec();

    /**
     * Resolves the actual bound port for the given virtual node.
     *
     * @param virtualNodeId the virtual node
     * @return the actual bound port
     * @throws IllegalStateException if the port cannot be resolved (e.g. port 0 before binding)
     */
    int resolvePort(ProxyNodeId virtualNodeId);

    /**
     * Binds the port resolver used by {@link #resolvePort(ProxyNodeId)}.
     * Called from {@code KafkaProxy.startup()} after endpoint registration.
     *
     * @param resolver maps a {@link ProxyNodeId} to its actual bound port
     */
    default void bindPortResolver(Function<ProxyNodeId, Integer> resolver) {
        // no-op for implementations that predate port resolution
    }

}
