/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Objects;

import io.kroxylicious.proxy.internal.routing.DirectRouting;
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
        if (!(endpointGateway().virtualCluster().routing() instanceof DirectRouting dr)) {
            throw new IllegalStateException("BootstrapEndpointBinding only has an upstream target for direct-routing virtual clusters");
        }
        return dr.targetCluster().bootstrapServer();
    }

    @Nullable
    @Override
    public Integer nodeId() {
        return null;
    }
}
