/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Represents a plugin instance's configuration as declared in a plugin YAML file.
 *
 * @param name The name of the plugin instance. Must match the external identity
 *             (e.g. the filename in a filesystem snapshot).
 * @param type The fully qualified class name of the plugin implementation.
 * @param version The version of the configuration.
 * @param shared Whether this instance should be shared across all consumers.
 *               Defaults to {@code false}. Only permitted on {@code @Stateless} plugins.
 * @param config The plugin's configuration.
 */
@JsonIgnoreProperties("$schema")
public record PluginConfig(
                           String name,
                           String type,
                           String version,
                           boolean shared,
                           Object config) {

    /**
     * Constructor without shared flag, defaulting to {@code false}.
     */
    public PluginConfig(
                        String name,
                        String type,
                        String version,
                        Object config) {
        this(name, type, version, false, config);
    }
}
