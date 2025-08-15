/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.service.HostPort;

/**
 * A bootstrap binding.
 *
 * @param endpointGateway the endpoint gateway
 */
public record MetadataDiscoveryEndpointBinding(EndpointGateway endpointGateway, Integer nodeId)
        implements NodeSpecificEndpointBinding {

    public MetadataDiscoveryEndpointBinding {
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
