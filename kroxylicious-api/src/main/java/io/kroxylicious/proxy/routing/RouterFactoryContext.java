/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.routing;

import java.util.Set;

import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

/**
 * Context for {@link RouterFactory} initialisation and router creation,
 * providing access to the plugin registry.
 */
public interface RouterFactoryContext {

    /**
     * Gets a plugin instance for the given plugin type and name.
     *
     * @param <P> the plugin type
     * @param pluginClass the plugin class
     * @param implementationName the plugin implementation name
     * @return the plugin instance
     * @throws UnknownPluginInstanceException if the named implementation is unknown
     */
    <P> P pluginInstance(Class<P> pluginClass, String implementationName);

    /**
     * Returns the implementation names of the registered instances
     * of the given plugin type.
     *
     * @param <P> the plugin type
     * @param pluginClass the plugin class
     * @return the set of known implementation names
     */
    <P> Set<String> pluginImplementationNames(Class<P> pluginClass);
}
