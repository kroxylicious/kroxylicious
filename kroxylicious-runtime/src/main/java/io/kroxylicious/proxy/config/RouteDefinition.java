/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

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

    private static final Pattern NAME_PATTERN = Pattern.compile("[a-z0-9A-Z](?:[a-z0-9A-Z_.-]{0,251}[a-z0-9A-Z])?");

    /**
     * Validates the route: {@code name} and {@code target} are required and {@code id}
     * must be non-negative.
     */
    @JsonCreator
    public RouteDefinition {
        Objects.requireNonNull(name, "'name' is required in a route definition");
        if (!NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid route name '" + name + "' (should match '" + NAME_PATTERN.pattern() + "')");
        }
        Objects.requireNonNull(target, "'target' is required in route '" + name + "'");
        if (id < 0) {
            throw new IllegalConfigurationException(
                    "Route '" + name + "' has invalid id " + id + ": must be >= 0");
        }
    }

    /**
     * The name of the cluster this route targets, if any.
     *
     * @return the target cluster name, or {@code null} if the route targets a router
     */
    @Nullable
    public String cluster() {
        return target.cluster();
    }

    /**
     * The name of the router this route targets, if any.
     *
     * @return the target router name, or {@code null} if the route targets a cluster
     */
    @Nullable
    public String router() {
        return target.router();
    }
}
