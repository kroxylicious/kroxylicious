/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

/**
 * A named filter definition
 * @param name The name of the filter instance.
 * @param type
 * @param config
 * @see Configuration#filterDefinitions()
 */
public record NamedFilterDefinition(
                                    @JsonProperty(required = true) String name,
                                    @PluginImplName(FilterFactory.class) @JsonProperty(required = true) String type,
                                    @PluginImplConfig(implNameProperty = "type") Object config) {

    @JsonCreator
    public NamedFilterDefinition {
        Objects.requireNonNull(name);
        // TODO should probably constrain the allowd chars in the name
        Objects.requireNonNull(type);
    }

    public FilterDefinition asFilterDefinition() {
        return new FilterDefinition(type, config);
    }

}
