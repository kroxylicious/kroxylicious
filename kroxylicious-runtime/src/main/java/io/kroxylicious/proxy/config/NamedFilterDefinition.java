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

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A named filter definition
 * @param name The name of the filter instance.
 * @param type The name of the {@link FilterFactory} plugin implementation providing the filter.
 * @param config The filter's configuration, or {@code null} if the filter requires none.
 * @see Configuration#filterDefinitions()
 */
public record NamedFilterDefinition(
                                    @JsonProperty(required = true) String name,
                                    @PluginImplName(FilterFactory.class) @JsonProperty(required = true) String type,
                                    @Nullable @PluginImplConfig(implNameProperty = "type") Object config) {

    private static final Pattern NAME_PATTERN = Pattern.compile("[a-z0-9A-Z](?:[a-z0-9A-Z_.-]{0,251}[a-z0-9A-Z])?");

    /**
     * Validates the filter definition: {@code name} must match the allowed name pattern
     * and {@code type} is required.
     */
    @JsonCreator
    public NamedFilterDefinition {
        Objects.requireNonNull(name);
        if (!NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid filter name '" + name + "' (should match '" + NAME_PATTERN.pattern() + "')");
        }
        Objects.requireNonNull(type);
    }

    /**
     * Returns {@code true} if this definition is semantically identical to {@code other}.
     * Used by the configuration change-detection pipeline (see {@code FilterChangeDetector})
     * to decide whether clusters referencing this filter need to be restarted during hot-reload.
     *
     * @param other the filter definition to compare against, may be {@code null}
     * @return true if the two definitions are semantically identical
     */
    public boolean sameAs(@Nullable NamedFilterDefinition other) {
        return Objects.equals(this, other);
    }

}
