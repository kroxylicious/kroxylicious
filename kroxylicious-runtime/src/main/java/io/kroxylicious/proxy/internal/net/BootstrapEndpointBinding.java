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
 * @param endpointListener the endpoint listener
 * @param upstreamTarget the upstream bootstrap target
 */
public record BootstrapEndpointBinding(EndpointListener endpointListener, HostPort upstreamTarget) implements EndpointBinding {

    public BootstrapEndpointBinding {
        Objects.requireNonNull(endpointListener, "endpointListener cannot be null");
        Objects.requireNonNull(upstreamTarget, "upstreamTarget cannot be null");
    }

    @Override
    public String toString() {
        return "BootstrapEndpointBinding[" +
                "endpointListener=" + this.endpointListener() + ", " +
                "upstreamTarget=" + this.upstreamTarget() + ']';
    }

}
