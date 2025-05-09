/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;
import java.util.regex.Pattern;

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

    private static final Pattern NAME_PATTERN = Pattern.compile("[a-z0-9A-Z](?:[a-z0-9A-Z_.-]{0,251}[a-z0-9A-Z])?");

    @JsonCreator
    public NamedFilterDefinition {
        Objects.requireNonNull(name);
        if (!NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid filter name '" + name + "' (should match '" + NAME_PATTERN.pattern() + "')");
        }
        Objects.requireNonNull(type);
    }

}
