/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import io.kroxylicious.proxy.filter.FilterFactoryContext;

import edu.umd.cs.findbugs.annotations.NonNull;

public class Plugins {
    private Plugins() {
    }

    /**
     * Checks that the given {@code config} is not null, throwing {@link PluginConfigurationException} if it is.
     * @param pluginImpl The plugin consuming the config
     * @param config The possibly null config
     * @return The non-null config
     * @param <C>
     */
    public static @NonNull <C> C requireConfig(Object pluginImpl, C config) {
        if (config == null) {
            throw new PluginConfigurationException(pluginImpl.getClass().getSimpleName() + " requires configuration, but config object is null");
        }
        return config;
    }

    /**
     * Creates an instance of a plugin interface and applies the given {@code configure} function to consume the configuration for it
     * @param context The context.
     * @param pluginInterface The plugin interface.
     * @param pluginImplName The name of the plugin implementation to instantiate.
     * @param pluginConfig The plugin configuration.
     * @param configure The function which applies the configuration to the instance.
     * @return The result of the {@code configure} function.
     * @param <P> The type of the plugin interface.
     * @param <C> The type of the config consumed by the instance.
     * @param <T> The result of applying the config.
     */
    public static <P, C, T> T applyConfiguration(FilterFactoryContext context, Class<P> pluginInterface,
                                                 String pluginImplName,
                                                 C pluginConfig,
                                                 BiFunction<P, C, T> configure) {
        P instance = context.pluginInstance(pluginInterface, pluginImplName);
        return configure.apply(instance, pluginConfig);
    }

    /**
     * Creates an instance of a plugin interface and applies the given {@code configure} function to consume the configuration for it
     * @param context The context.
     * @param pluginInterface The plugin interface.
     * @param pluginImplName The name of the plugin implementation to instantiate.
     * @param pluginConfig The plugin configuration.
     * @param configure The function which applies the configuration to the instance.
     *
     * @param <P> The type of the plugin interface.
     * @param <C> The type of the config consumed by the instance.
     */
    public static <P, C> void acceptConfiguration(FilterFactoryContext context,
                                                  Class<P> pluginInterface,
                                                  String pluginImplName,
                                                  C pluginConfig,
                                                  BiConsumer<P, C> configure) {
        P factory = context.pluginInstance(pluginInterface, pluginImplName);
        configure.accept(factory, pluginConfig);
    }
}
