/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.ScheduledExecutorService;

import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;
import io.kroxylicious.proxy.plugin.UnknownPluginTypeException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Construction context for Filters. Used to pass the filter configuration and environmental resources
 * to the FilterFactory when it is creating a new instance of the Filter. see {@link FilterFactory#createFilter(FilterFactoryContext, Object)}
 */
public interface FilterFactoryContext {

    /**
     * The event loop that this Filter's channel is assigned to.
     * Null if the factory is not bound to a channel yet.
     * Should be safe
     * to mutate Filter members from this executor.
     * @return executor, or null
     */
    ScheduledExecutorService eventLoop();

    /**
     * Gets a plugin instance for the given plugin type and name
     * @param pluginClass The plugin type
     * @param instanceName The plugin instance name
     * @return The plugin instance
     * @param <P> The plugin manager type
     * @throws UnknownPluginTypeException
     * @throws UnknownPluginInstanceException
     */
    <P> @NonNull P pluginInstance(@NonNull Class<P> pluginClass, @NonNull String instanceName);
}
