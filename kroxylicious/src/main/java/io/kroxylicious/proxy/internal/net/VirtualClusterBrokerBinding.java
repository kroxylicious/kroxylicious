/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;

/**
 * A broker specific virtual cluster binding.
 */
public record VirtualClusterBrokerBinding(VirtualCluster virtualCluster, HostPort upstreamTarget, int nodeId) implements VirtualClusterBinding {
    public VirtualClusterBrokerBinding {
        Objects.requireNonNull(virtualCluster, "virtualCluster must not be null");
        Objects.requireNonNull(upstreamTarget, "upstreamTarget must not be null");
    }

    @Override
    public String toString() {
        return "VirtualClusterBrokerBinding[" +
                "virtualCluster=" + this.virtualCluster() + ", " +
                "upstreamTarget=" + this.upstreamTarget() + ", " +
                "nodeId=" + nodeId + ']';
    }


    public boolean referToSameVirtualClusterNode(VirtualClusterBrokerBinding other) {
        return other != null && Objects.equals(other.nodeId, this.nodeId) && Objects.equals(other.virtualCluster, this.virtualCluster);
    }

}
