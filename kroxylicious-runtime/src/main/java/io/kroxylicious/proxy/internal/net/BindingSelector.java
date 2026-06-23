/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Selects the {@link EndpointBinding} for an incoming connection from the set of
 * bindings registered on an acceptor channel, and controls how acceptor channels
 * are shared between bindings.
 * <p>
 * The {@link #select} method determines routing: how an incoming connection is
 * matched to the correct binding. The {@link #channelKey} method determines
 * channel allocation: whether bindings share an acceptor channel or each get
 * their own.
 * <p>
 * Implementations are obtained via the constructors of the permitted subtypes
 * and set on each acceptor channel during
 * {@link EndpointRegistry#registerVirtualCluster registration}.
 * {@link EndpointGateway#bindingSelector()} determines which selector a gateway uses.
 */
sealed interface BindingSelector permits BindingSelector.PortPerNode, BindingSelector.Sni {

    /**
     * Selects a binding for the incoming connection.
     *
     * @param bindings     the bindings registered on the acceptor channel
     * @param acceptorPort the local port the connection arrived on
     * @param sniHostname  the TLS SNI hostname, or {@code null} for plain connections
     * @return the selected binding, or {@code null} if no binding matches
     * @throws EndpointResolutionException if selection is ambiguous
     */
    @Nullable
    EndpointBinding select(Map<EndpointRegistry.RoutingKey, EndpointBinding> bindings, int acceptorPort, @Nullable String sniHostname);

    /**
     * Returns the key to use in the listening-channels map for the given configured
     * endpoint. This controls whether bindings share an acceptor channel: bindings
     * that return the same key share one channel.
     *
     * @param configured the configured endpoint (may have port 0 for OS-assigned)
     * @return the key to use for channel lookup/creation
     */
    Endpoint channelKey(Endpoint configured);

    /**
     * Selector for port-per-node gateways: each acceptor has a single binding
     * under the null routing key. When port is OS-assigned (0), each binding
     * gets a dedicated channel via a unique synthetic key.
     */
    record PortPerNode() implements BindingSelector {
        private static final AtomicInteger SYNTHETIC_PORT_COUNTER = new AtomicInteger(Integer.MIN_VALUE);

        @Override
        @Nullable
        public EndpointBinding select(Map<EndpointRegistry.RoutingKey, EndpointBinding> bindings, int acceptorPort, @Nullable String sniHostname) {
            return bindings.get(EndpointRegistry.RoutingKey.NULL_ROUTING_KEY);
        }

        @Override
        public Endpoint channelKey(Endpoint configured) {
            return configured.port() == 0
                    ? Endpoint.createEndpoint(configured.bindingAddress(), SYNTHETIC_PORT_COUNTER.getAndDecrement(), configured.tls())
                    : configured;
        }
    }

    /**
     * Selector for SNI gateways: matches the SNI hostname against registered
     * bindings, falling back to broker-address-pattern matching for connections
     * targeting a broker node not yet seen in a Metadata response.
     * All bindings share the same acceptor channel regardless of port value.
     */
    record Sni() implements BindingSelector {
        @Override
        @Nullable
        public EndpointBinding select(Map<EndpointRegistry.RoutingKey, EndpointBinding> bindings, int acceptorPort, @Nullable String sniHostname) {
            var binding = bindings.getOrDefault(EndpointRegistry.RoutingKey.createBindingKey(sniHostname), bindings.get(EndpointRegistry.RoutingKey.NULL_ROUTING_KEY));
            if (binding != null) {
                return binding;
            }
            if (sniHostname != null) {
                var brokerAddress = new HostPort(sniHostname, acceptorPort);
                HashMap<BootstrapEndpointBinding, Integer> matches = new HashMap<>();
                bindings.values().stream()
                        .filter(BootstrapEndpointBinding.class::isInstance)
                        .map(BootstrapEndpointBinding.class::cast)
                        .forEach(b -> {
                            var nodeId = b.endpointGateway().getBrokerIdFromBrokerAddress(brokerAddress);
                            if (nodeId != null) {
                                matches.put(b, nodeId);
                            }
                        });
                if (matches.size() > 1) {
                    throw new EndpointResolutionException(
                            "Failed to generate an unbound broker binding from SNI " +
                                    "as it matches the broker address pattern of more than one virtual cluster");
                }
                if (matches.size() == 1) {
                    var e = matches.entrySet().iterator().next();
                    return new MetadataDiscoveryBrokerEndpointBinding(e.getKey().endpointGateway(), e.getValue());
                }
            }
            return null;
        }

        @Override
        public Endpoint channelKey(Endpoint configured) {
            return configured;
        }
    }
}
