/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.service.HostPort;

/**
 * A binding to a virtual cluster bootstrap.
 *
 * @param virtualClusterModel the virtual cluster
 * @param upstreamTarget the upstream bootstrap target
 */
public record VirtualClusterBootstrapBinding(EndpointListener virtualClusterModel, HostPort upstreamTarget) implements VirtualClusterBinding {

    public VirtualClusterBootstrapBinding {
        Objects.requireNonNull(virtualClusterModel, "virtualCluster cannot be null");
        Objects.requireNonNull(upstreamTarget, "upstreamTarget cannot be null");
    }

    @Override
    public String toString() {
        return "VirtualClusterBrokerBinding[" +
                "virtualCluster=" + this.virtualClusterModel() + ", " +
                "upstreamTarget=" + this.upstreamTarget() + ']';
    }

}
