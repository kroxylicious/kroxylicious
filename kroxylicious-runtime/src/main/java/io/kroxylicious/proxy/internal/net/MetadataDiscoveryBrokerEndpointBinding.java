/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.internal.routing.DirectRouting;
import io.kroxylicious.proxy.service.HostPort;

/**
 * A placeholder endpoint binding used before the proxy has learned the real upstream address
 * for a broker node. It is created when a per-node port is bound but the upstream topology
 * has not yet been reconciled.
 *
 * <p>The {@link #upstreamTarget()} method returns the cluster's bootstrap server address
 * as a stand-in, allowing the connection to proceed far enough to fire a Metadata request
 * (via {@link io.kroxylicious.proxy.internal.filter.impl.EagerMetadataLearner}) and
 * trigger topology discovery. Once reconciliation completes, this binding is superseded by
 * a {@link BrokerEndpointBinding} pointing at the real broker address.
 *
 * <p><strong>Direct routing only.</strong> {@link #upstreamTarget()} is only meaningful for
 * virtual clusters using {@link io.kroxylicious.proxy.internal.routing.DirectRouting}.
 * Dynamic routing virtual clusters resolve upstream addresses through the router's
 * {@link io.kroxylicious.proxy.internal.routing.RoutingHandler} on a per-request basis
 * and have no use for this binding's upstream target. Calling {@link #upstreamTarget()} on
 * a dynamic routing virtual cluster throws {@link IllegalStateException}.
 *
 * <p>Note: instances of this class can be produced for any virtual cluster type whose gateway
 * declares per-node ports (see {@link io.kroxylicious.proxy.internal.net.EndpointRegistry}).
 * Callers must therefore check the routing type before acting on
 * {@link #restrictUpstreamToMetadataDiscovery()}.
 *
 * @param endpointGateway the endpoint gateway
 * @param nodeId kafka nodeId of the target broker
 */
public record MetadataDiscoveryBrokerEndpointBinding(EndpointGateway endpointGateway, Integer nodeId)
        implements NodeSpecificEndpointBinding {

    /**
     * Creates a metadata discovery broker endpoint binding.
     *
     * @param endpointGateway the endpoint gateway
     * @param nodeId kafka nodeId of the target broker
     */
    public MetadataDiscoveryBrokerEndpointBinding {
        Objects.requireNonNull(endpointGateway, "endpointGateway cannot be null");
        Objects.requireNonNull(nodeId, "nodeId must not be null");
    }

    @Override
    public HostPort upstreamTarget() {
        if (!(endpointGateway.virtualCluster().routing() instanceof DirectRouting dr)) {
            throw new IllegalStateException("upstreamTarget() requires direct routing, but virtual cluster is using dynamic routing");
        }
        return dr.upstreamCluster().bootstrapServer();
    }

    @Override
    public boolean restrictUpstreamToMetadataDiscovery() {
        return true;
    }
}
