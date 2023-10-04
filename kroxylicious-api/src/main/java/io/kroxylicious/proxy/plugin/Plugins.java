/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

public class Plugins {
    private Plugins() {
    }

    public static @NonNull <C> C requireConfig(Object pluginImpl, C config) {
        if (config == null) {
            throw new PluginConfigurationException(pluginImpl.getClass().getSimpleName() + " requires configuration, but config object is null");
        }
        return config;
    }
}
