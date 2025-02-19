/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.service.HostPort;

/**
 * A broker specific virtual cluster binding.
 *
 * @param endpointListener                       the virtual cluster
 * @param upstreamTarget                       the upstream target of this binding
 * @param nodeId                               kafka nodeId of the target broker
 * @param restrictUpstreamToMetadataDiscovery  true if the upstreamTarget corresponds to a broker, false if it points at a bootstrap.
 */
public record VirtualClusterBrokerBinding(EndpointListener endpointListener, HostPort upstreamTarget, int nodeId, boolean restrictUpstreamToMetadataDiscovery)
        implements VirtualClusterBinding {
    public VirtualClusterBrokerBinding {
        Objects.requireNonNull(endpointListener, "virtualCluster must not be null");
        Objects.requireNonNull(upstreamTarget, "upstreamTarget must not be null");
    }

    @Override
    public String toString() {
        return "VirtualClusterBrokerBinding[" +
                "virtualCluster=" + this.endpointListener() + ", " +
                "upstreamTarget=" + this.upstreamTarget() + ", " +
                "restrictUpstreamToMetadataDiscovery=" + this.restrictUpstreamToMetadataDiscovery() + ", " +
                "nodeId=" + nodeId + ']';
    }

    public boolean refersToSameVirtualClusterAndNode(VirtualClusterBrokerBinding other) {
        return other != null && other.nodeId == this.nodeId && Objects.equals(other.endpointListener, this.endpointListener);
    }

}
