/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import io.kroxylicious.proxy.micrometer.MicrometerConfigurationHookService;
import io.kroxylicious.proxy.plugin.PluginReference;

/**
 * A compact reference to a MicrometerConfigurationHookService plugin instance, deserialised from a bare string in YAML.
 */
public record MicrometerRef(String pluginInstance) {

    private static final String MICROMETER_INTERFACE = MicrometerConfigurationHookService.class.getName();

    @JsonCreator
    public static MicrometerRef of(String pluginInstance) {
        return new MicrometerRef(pluginInstance);
    }

    @JsonValue
    @Override
    public String pluginInstance() {
        return pluginInstance;
    }

    /** Converts to a {@link PluginReference} with the MicrometerConfigurationHookService interface type. */
    public PluginReference<MicrometerConfigurationHookService> toReference() {
        return new PluginReference<>(MICROMETER_INTERFACE, pluginInstance);
    }
}
