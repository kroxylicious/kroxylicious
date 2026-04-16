/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Map;
import java.util.Set;

import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

/**
 * A PluginFactory is able to resolve references to a plugin implementation (i.e. a name) to that implementation.
 * @param <P> The plugin type
 */
public interface PluginFactory<P> {

    /**
     * Resolves a plugin reference to a plugin implementation.
     * @param instanceName The name of the plugin implementation
     * @return The plugin implementation
     * @throws UnknownPluginInstanceException If the plugin implementation with the given name could not be found
     */
    P pluginInstance(String instanceName);

    /**
     * Resolves a plugin reference to the plugins config type.
     * @param instanceName The name of the plugin implementation
     * @return The plugin's config type'
     * @throws UnknownPluginInstanceException If the plugin implementation with the given name could not be found
     */
    Class<?> configType(String instanceName);

    /**
     * Returns the supported config versions and their corresponding config types for a plugin implementation.
     * The map keys are config version strings (e.g. {@code "v1alpha1"}, {@code "v1"}).
     * An empty string key represents the legacy (unversioned) config type.
     *
     * @param instanceName The name of the plugin implementation
     * @return An immutable map of config version to config type
     * @throws UnknownPluginInstanceException If the plugin implementation with the given name could not be found
     */
    Map<String, Class<?>> configVersions(String instanceName);

    /**
     * Returns the implementation class for the given plugin instance name.
     *
     * @param instanceName The name of the plugin implementation
     * @return The implementation class
     * @throws UnknownPluginInstanceException If the plugin implementation with the given name could not be found
     */
    Class<?> implementationType(String instanceName);

    /**
     * Returns the names of the registered instances of this plugin.
     * The set is immutable.
     */
    Set<String> registeredInstanceNames();

}
