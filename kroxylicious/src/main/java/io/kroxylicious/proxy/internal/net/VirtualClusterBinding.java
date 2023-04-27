/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.config.VirtualCluster;

/**
 * Used to represent a binding from an @{@link Endpoint} to a @{@link VirtualCluster}.
 * This is the broker-less binding and is used to represent the bootstrap.
 */
public class VirtualClusterBinding {
    private final VirtualCluster virtualCluster;

    public VirtualClusterBinding(VirtualCluster virtualCluster) {
        Objects.requireNonNull(virtualCluster, "virtualCluster cannot be null");
        this.virtualCluster = virtualCluster;
    }

    public VirtualCluster virtualCluster() {
        return virtualCluster;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (VirtualClusterBinding) obj;
        return Objects.equals(this.virtualCluster, that.virtualCluster);
    }

    @Override
    public int hashCode() {
        return Objects.hash(virtualCluster);
    }

    @Override
    public String toString() {
        return "VirtualClusterBinding[" +
                "virtualCluster=" + virtualCluster + ']';
    }

    public static VirtualClusterBinding createBinding(VirtualCluster virtualCluster) {
        return createBinding(virtualCluster, null);
    }

    public static VirtualClusterBinding createBinding(VirtualCluster virtualCluster, Integer nodeId) {
        return nodeId == null ? new VirtualClusterBinding(virtualCluster) : new VirtualClusterBrokerBinding(virtualCluster, nodeId);
    }

}
