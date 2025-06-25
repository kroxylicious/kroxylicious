/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import edu.umd.cs.findbugs.annotations.Nullable;

public class Plugins {
    private Plugins() {
    }

    /**
     * Checks that the given {@code config} is not null, throwing {@link PluginConfigurationException} if it is.
     * @param pluginImpl The plugin consuming the config
     * @param config The possibly null config
     * @return The non-null config
     * @param <C> The type of the config
     */
    public static <C> C requireConfig(Object pluginImpl, @Nullable C config) {
        if (config == null) {
            throw new PluginConfigurationException(pluginImpl.getClass().getSimpleName() + " requires configuration, but config object is null");
        }
        return config;
    }
}
