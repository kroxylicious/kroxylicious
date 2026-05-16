/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A route within a router definition, targeting either a named target cluster
 * or another router.
 *
 * @param name unique name within the enclosing router
 * @param filters optional list of filter names applied to requests on this route
 * @param targetCluster name of a target cluster (mutually exclusive with {@code router})
 * @param router name of another router (mutually exclusive with {@code targetCluster})
 */
public record RouteDefinition(
                              @JsonProperty(required = true) String name,
                              @Nullable List<String> filters,
                              @Nullable String targetCluster,
                              @Nullable String router) {

    @JsonCreator
    public RouteDefinition {
        Objects.requireNonNull(name, "'name' is required in a route definition");
        if (targetCluster != null && router != null) {
            throw new IllegalConfigurationException(
                    "Route '" + name + "' must specify exactly one of 'targetCluster' or 'router', but both were provided");
        }
        if (targetCluster == null && router == null) {
            throw new IllegalConfigurationException(
                    "Route '" + name + "' must specify exactly one of 'targetCluster' or 'router', but neither was provided");
        }
    }
}
