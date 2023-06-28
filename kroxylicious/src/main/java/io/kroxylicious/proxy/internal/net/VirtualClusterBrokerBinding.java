/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;

/**
 * A broker specific virtual cluster binding.
 */
public record VirtualClusterBrokerBinding(VirtualCluster virtualCluster, HostPort upstreamTarget, int nodeId, boolean uninitializedBrokers) implements VirtualClusterBinding {
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


    public boolean refersToSameVirtualClusterAndNode(VirtualClusterBrokerBinding other) {
        return other != null && other.nodeId == this.nodeId && Objects.equals(other.virtualCluster, this.virtualCluster);
    }

}
