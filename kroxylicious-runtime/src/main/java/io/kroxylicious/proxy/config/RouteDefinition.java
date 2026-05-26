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
 * A route within a router definition.
 *
 * @param name unique name within the enclosing router
 * @param filters optional list of filter names applied to requests on this route
 * @param target the route's target (a cluster or another router)
 */
public record RouteDefinition(
                              @JsonProperty(required = true) String name,
                              @Nullable List<String> filters,
                              @JsonProperty(required = true) Target target) {

    @JsonCreator
    public RouteDefinition {
        Objects.requireNonNull(name, "'name' is required in a route definition");
        Objects.requireNonNull(target, "'target' is required in route '" + name + "'");
    }

    /**
     * @return the name of the target cluster, or {@code null} if this route targets a router
     */
    @Nullable
    public String cluster() {
        return target.cluster();
    }

    /**
     * @return the name of the target router, or {@code null} if this route targets a cluster
     */
    @Nullable
    public String router() {
        return target.router();
    }

    /**
     * A discriminated union: exactly one of {@code cluster} or {@code router} must be specified.
     *
     * @param cluster name of a cluster defined in {@code clusterDefinitions}
     * @param router name of another router defined in {@code routerDefinitions}
     */
    public record Target(@Nullable String cluster,
                         @Nullable String router) {

        @JsonCreator
        public Target {
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
}
