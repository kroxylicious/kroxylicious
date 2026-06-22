/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.HashMap;
import java.util.Map;

import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Selects the {@link EndpointBinding} for an incoming connection from the set of
 * bindings registered on an acceptor channel. The selection strategy depends on
 * the gateway type — port-per-node gateways have one binding per acceptor,
 * while SNI gateways multiplex several bindings on a shared port.
 * <p>
 * Implementations are obtained via the static factory methods and set on each
 * acceptor channel during {@link EndpointRegistry#registerVirtualCluster registration}.
 * {@link EndpointGateway#bindingSelector()} determines which selector a gateway uses.
 */
@FunctionalInterface
interface BindingSelector {

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
     * Selector for port-per-node gateways: each acceptor has a single binding
     * under the null routing key.
     */
    static BindingSelector portPerNode() {
        return (bindings, port, sni) -> bindings.get(EndpointRegistry.RoutingKey.NULL_ROUTING_KEY);
    }

    /**
     * Selector for SNI gateways: matches the SNI hostname against registered
     * bindings, falling back to broker-address-pattern matching for connections
     * targeting a broker node not yet seen in a Metadata response.
     */
    static BindingSelector sni() {
        return BindingSelector::selectBySni;
    }

    @Nullable
    private static EndpointBinding selectBySni(Map<EndpointRegistry.RoutingKey, EndpointBinding> bindings, int acceptorPort, @Nullable String sniHostname) {
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
}
