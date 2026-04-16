/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.plugin.PluginReference;

/**
 * A compact reference to a FilterFactory plugin instance, deserialised from a bare string in YAML.
 */
public record FilterFactoryRef(String pluginInstance) {

    private static final String FILTER_FACTORY_INTERFACE = FilterFactory.class.getName();

    @JsonCreator
    public static FilterFactoryRef of(String pluginInstance) {
        return new FilterFactoryRef(pluginInstance);
    }

    @JsonValue
    @Override
    public String pluginInstance() {
        return pluginInstance;
    }

    /** Converts to a {@link PluginReference} with the FilterFactory interface type. */
    public PluginReference<FilterFactory> toReference() {
        return new PluginReference<>(FILTER_FACTORY_INTERFACE, pluginInstance);
    }
}
