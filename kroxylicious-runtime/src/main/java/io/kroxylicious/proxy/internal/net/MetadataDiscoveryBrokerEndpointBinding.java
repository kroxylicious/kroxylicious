/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.service.HostPort;

/**
 * A bootstrap binding which can only be used for metadata discovery.
 * <p>
 * Its role is to allow the proxy to bind known ports without knowing the full upstream topology.
 * Once the upstream topology is discovered this should be replaced with a {@link BrokerEndpointBinding} for the same nodeId
 * @param endpointGateway the endpoint gateway
 * @param nodeId kafka nodeId of the target broker
 */
public record MetadataDiscoveryBrokerEndpointBinding(EndpointGateway endpointGateway, Integer nodeId)
        implements NodeSpecificEndpointBinding {

    public MetadataDiscoveryBrokerEndpointBinding {
        Objects.requireNonNull(endpointGateway, "endpointGateway cannot be null");
        Objects.requireNonNull(nodeId, "nodeId must not be null");
    }

    @Override
    public HostPort upstreamTarget() {
        return endpointGateway.targetCluster().bootstrapServer();
    }

    @Override
    public Integer nodeId() {
        return nodeId;
    }

    @Override
    public boolean restrictUpstreamToMetadataDiscovery() {
        return true;
    }
}
