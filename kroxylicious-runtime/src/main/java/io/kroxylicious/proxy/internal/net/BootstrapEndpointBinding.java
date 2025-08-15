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
 */
public record BootstrapEndpointBinding(EndpointGateway endpointGateway) implements EndpointBinding {

    public BootstrapEndpointBinding {
        Objects.requireNonNull(endpointGateway, "endpointGateway cannot be null");
    }

    @Override
    public HostPort upstreamTarget() {
        return endpointGateway().targetCluster().bootstrapServer();
    }

    @Nullable
    @Override
    public Integer nodeId() {
        return null;
    }
}
