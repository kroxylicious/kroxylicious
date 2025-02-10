/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

/**
 * A broker specific virtual cluster binding.
 *
 * @param virtualClusterModel                       the virtual cluster
 * @param upstreamTarget                       the upstream target of this binding
 * @param nodeId                               kafka nodeId of the target broker
 * @param restrictUpstreamToMetadataDiscovery  true if the upstreamTarget corresponds to a broker, false if it points at a bootstrap.
 */
public record VirtualClusterBrokerBinding(VirtualClusterModel virtualClusterModel, HostPort upstreamTarget, int nodeId, boolean restrictUpstreamToMetadataDiscovery)
        implements VirtualClusterBinding {
    public VirtualClusterBrokerBinding {
        Objects.requireNonNull(virtualClusterModel, "virtualCluster must not be null");
        Objects.requireNonNull(upstreamTarget, "upstreamTarget must not be null");
    }

    @Override
    public String toString() {
        return "VirtualClusterBrokerBinding[" +
                "virtualCluster=" + this.virtualClusterModel() + ", " +
                "upstreamTarget=" + this.upstreamTarget() + ", " +
                "restrictUpstreamToMetadataDiscovery=" + this.restrictUpstreamToMetadataDiscovery() + ", " +
                "nodeId=" + nodeId + ']';
    }

    public boolean refersToSameVirtualClusterAndNode(VirtualClusterBrokerBinding other) {
        return other != null && other.nodeId == this.nodeId && Objects.equals(other.virtualClusterModel, this.virtualClusterModel);
    }

}
