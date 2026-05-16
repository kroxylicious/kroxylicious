/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.routing;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.UnknownNullness;

/**
 * A pluggable source of {@link Router} instances.
 *
 * <p>RouterFactories are {@linkplain java.util.ServiceLoader service}
 * implementations provided by router authors and called by the proxy
 * runtime to create router instances.</p>
 *
 * <p>The proxy runtime guarantees that:</p>
 * <ol>
 *   <li>instances will be {@linkplain #initialize initialised} before
 *       any attempt to {@linkplain #createRouter create} router instances,</li>
 *   <li>instances will eventually be {@linkplain #close closed} if and only
 *       if they were successfully initialised,</li>
 *   <li>no attempts to create router instances will be made once a
 *       {@code RouterFactory} instance is closed,</li>
 *   <li>instances will be initialised and closed on the same thread.</li>
 * </ol>
 *
 * @param <C> the configuration type. Use {@link Void} if not configurable.
 * @param <I> the initialisation data type
 */
public interface RouterFactory<C, I> {

    /**
     * Initialises the factory with the given configuration.
     *
     * @param context the factory context
     * @param config the configuration
     * @return initialisation data to be passed to {@link #createRouter}
     * @throws PluginConfigurationException if the configuration is invalid
     */
    @UnknownNullness
    I initialize(
                 RouterFactoryContext context,
                 @UnknownNullness C config)
            throws PluginConfigurationException;

    /**
     * Creates a new router instance.
     *
     * <p>This may be called on a different thread from
     * {@link #initialize} and {@link #close}.</p>
     *
     * @param context the factory context
     * @param initializationData the data returned from {@link #initialize}
     * @return a new router instance
     */
    Router createRouter(
                        RouterFactoryContext context,
                        @UnknownNullness I initializationData);

    /**
     * Releases resources associated with the given initialisation data.
     *
     * @param initializationData the data returned from {@link #initialize}
     */
    default void close(@UnknownNullness I initializationData) {
    }
}
