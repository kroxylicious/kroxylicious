/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Objects;

import io.kroxylicious.proxy.config.TargetCluster;

/**
 * Routing model for a virtual cluster that forwards directly to a single, statically-configured
 * upstream Kafka cluster.
 */
public record DirectRouting(TargetCluster targetCluster) implements RoutingModel {

    public DirectRouting {
        Objects.requireNonNull(targetCluster, "targetCluster");
    }
}
