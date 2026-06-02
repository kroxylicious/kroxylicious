/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A route within a router definition.
 *
 * @param name unique name within the enclosing router
 * @param id numeric identifier for this route, used in the virtual node ID mapping formula
 * @param filters optional list of filter names applied to requests on this route
 * @param target the route's target (a cluster or another router)
 */
public record RouteDefinition(
                              @JsonProperty(required = true) String name,
                              @JsonInclude(JsonInclude.Include.ALWAYS) @JsonProperty(required = true) int id,
                              @Nullable List<String> filters,
                              @JsonProperty(required = true) RouteTarget target) {

    @JsonCreator
    public RouteDefinition {
        Objects.requireNonNull(name, "'name' is required in a route definition");
        Objects.requireNonNull(target, "'target' is required in route '" + name + "'");
        if (id < 0) {
            throw new IllegalConfigurationException(
                    "Route '" + name + "' has invalid id " + id + ": must be >= 0");
        }
    }

    @Nullable
    public String cluster() {
        return target.cluster();
    }

    @Nullable
    public String router() {
        return target.router();
    }
}
