/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.routing.RouterFactory;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A named router definition referencing a {@link RouterFactory} plugin type.
 *
 * @param name unique name for this router
 * @param type the {@link RouterFactory} plugin implementation name
 * @param config optional plugin-specific configuration
 * @param routes the routes available from this router
 */
public record RouterDefinition(
                               @JsonProperty(required = true) String name,
                               @PluginImplName(RouterFactory.class) @JsonProperty(required = true) String type,
                               @Nullable @PluginImplConfig(implNameProperty = "type") Object config,
                               @JsonProperty(required = true) List<RouteDefinition> routes) {

    @JsonCreator
    public RouterDefinition {
        Objects.requireNonNull(name, "'name' is required in a router definition");
        Objects.requireNonNull(type, "'type' is required in a router definition");
        if (routes == null || routes.isEmpty()) {
            throw new IllegalConfigurationException("Router '" + name + "' must have at least one route");
        }
        var routeNames = routes.stream()
                .map(RouteDefinition::name)
                .toList();
        var duplicates = routeNames.stream()
                .filter(n -> Collections.frequency(routeNames, n) > 1)
                .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalConfigurationException(
                    "Router '" + name + "' has duplicate route names: " + duplicates);
        }
    }
}
