/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A broker specific endpoint binding.
 *
 * @param endpointGateway                      the endpoint listener
 * @param upstreamTarget                       the upstream target of this binding
 * @param nodeId                               kafka nodeId of the target broker
 * @param restrictUpstreamToMetadataDiscovery  true if the upstreamTarget corresponds to a broker, false if it points at a bootstrap.
 */
public record BrokerEndpointBinding(EndpointGateway endpointGateway, HostPort upstreamTarget, Integer nodeId, boolean restrictUpstreamToMetadataDiscovery)
        implements EndpointBinding {
    public BrokerEndpointBinding {
        Objects.requireNonNull(endpointGateway, "endpointGateway must not be null");
        Objects.requireNonNull(upstreamTarget, "upstreamTarget must not be null");
        Objects.requireNonNull(nodeId, "nodeId must not be null");
    }

    @Override
    @NonNull
    public Integer nodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "BrokerEndpointBinding[" +
                "endpointGateway=" + this.endpointGateway() + ", " +
                "upstreamTarget=" + this.upstreamTarget() + ", " +
                "restrictUpstreamToMetadataDiscovery=" + this.restrictUpstreamToMetadataDiscovery() + ", " +
                "nodeId=" + nodeId + ']';
    }

    public boolean refersToSameVirtualClusterAndNode(BrokerEndpointBinding other) {
        return Objects.equals(other.nodeId, nodeId) && Objects.equals(other.endpointGateway, this.endpointGateway);
    }

}
