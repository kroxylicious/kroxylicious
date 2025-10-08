/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Set;

import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

/**
 * Construction context for Filters. Used to pass the filter configuration and environmental resources
 * to the FilterFactory when it is creating a new instance of the Filter. see {@link FilterFactory#createFilter(FilterFactoryContext, Object)}
 */
public interface FilterFactoryContext {

    /**
     * An executor backed by the single Thread responsible for dispatching
     * work to a Filter instance for a channel.
     * It is safe to mutate Filter members from this executor.
     * @return executor
     * @throws IllegalStateException if the factory is not bound to a channel yet.
     */
    FilterDispatchExecutor filterDispatchExecutor();

    /**
     * Gets a plugin instance for the given plugin type and name
     *
     * @param <P> The plugin manager type
     * @param pluginClass The plugin type
     * @param implementationName The plugin implementation name
     * @return The plugin instance
     * @throws UnknownPluginInstanceException the plugin with given implementation name is unknown
     */
    <P> P pluginInstance(Class<P> pluginClass, String implementationName);

    /**
     * Returns the implementation names of the registered instances of this plugin.
     *
     * @param pluginClass The plugin type
     * @return set of known implementation names registered for the given plugin class.
     * @param <P> The plugin manager type
     */
    <P> Set<String> pluginImplementationNames(Class<P> pluginClass);
}
