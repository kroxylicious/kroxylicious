/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A target for a route: exactly one of {@code cluster} or {@code router} must be specified.
 *
 * @param cluster name of a cluster defined in {@code clusterDefinitions}
 * @param router name of a router defined in {@code routerDefinitions}
 */
public record RouteTarget(@Nullable String cluster,
                          @Nullable String router) {

    @JsonCreator
    public RouteTarget {
        if (cluster != null && router != null) {
            throw new IllegalConfigurationException(
                    "Route target must specify exactly one of 'cluster' or 'router', but both were provided");
        }
        if (cluster == null && router == null) {
            throw new IllegalConfigurationException(
                    "Route target must specify exactly one of 'cluster' or 'router', but neither was provided");
        }
    }
}
