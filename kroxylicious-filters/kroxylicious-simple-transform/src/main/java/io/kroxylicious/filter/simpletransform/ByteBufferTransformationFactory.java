/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

/**
 * A pluggable factory for {@link ByteBufferTransformation} instances.
 * @param <C> The type of the configuration object.
 */
public interface ByteBufferTransformationFactory<C> {

    /**
     * Validates the configuration.
     * @param config configuration
     * @throws PluginConfigurationException when the configuration is invalid
     */
    void validateConfiguration(C config) throws PluginConfigurationException;

    /**
     * Checks that the given configuration is non-null.
     * @param config The configuration to check.
     * @return The given configuration.
     * @throws PluginConfigurationException If the given configuration is null.
     */
    default C requireConfig(C config) {
        if (config == null) {
            throw new PluginConfigurationException(this.getClass().getSimpleName() + " requires configuration, but config object is null");
        }
        return config;
    }

    /**
     * Creates a transformation from the given configuration.
     * @param configuration The configuration.
     * @return The transformation.
     */
    ByteBufferTransformation createTransformation(C configuration);

}
