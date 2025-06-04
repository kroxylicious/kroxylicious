/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A bootstrap binding.
 *
 * @param endpointGateway the endpoint gateway
 * @param upstreamTarget the upstream bootstrap target
 */
public record BootstrapEndpointBinding(EndpointGateway endpointGateway, HostPort upstreamTarget) implements EndpointBinding {

    public BootstrapEndpointBinding {
        Objects.requireNonNull(endpointGateway, "endpointGateway cannot be null");
        Objects.requireNonNull(upstreamTarget, "upstreamTarget cannot be null");
    }

    @Nullable
    @Override
    public Integer nodeId() {
        return null;
    }

    @Override
    public String toString() {
        return "BootstrapEndpointBinding[" +
                "endpointGateway=" + this.endpointGateway() + ", " +
                "upstreamTarget=" + this.upstreamTarget() + ']';
    }
}
