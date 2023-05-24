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
 * Used to represent a binding from an @{@link Endpoint} to a @{@link VirtualCluster}.
 * This is the broker-less binding and is used to represent the bootstrap.
 */
public class VirtualClusterBinding {
    private final VirtualCluster virtualCluster;
    private final HostPort upstreamTarget;

    public VirtualClusterBinding(VirtualCluster virtualCluster, HostPort upstreamTarget) {
        Objects.requireNonNull(virtualCluster, "virtualCluster cannot be null");
        Objects.requireNonNull(upstreamTarget, "upstreamTarget cannot be null");
        this.virtualCluster = virtualCluster;
        this.upstreamTarget = upstreamTarget;
    }

    public VirtualCluster virtualCluster() {
        return virtualCluster;
    }

    public HostPort getUpstreamTarget() {
        return upstreamTarget;
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
        return Objects.equals(this.virtualCluster, that.virtualCluster) && Objects.equals(upstreamTarget, upstreamTarget);
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

}
