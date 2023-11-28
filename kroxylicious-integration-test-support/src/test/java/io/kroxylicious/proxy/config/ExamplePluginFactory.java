/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface ExamplePluginFactory<C> {

    @NonNull
    default C requireConfig(C config) {
        if (config == null) {
            throw new PluginConfigurationException(this.getClass().getSimpleName() + " requires configuration, but config object is null");
        }
        return config;
    }

    ExamplePlugin createExamplePlugin(C configuration);

    @FunctionalInterface
    interface ExamplePlugin {
        void foo();
    }
}
