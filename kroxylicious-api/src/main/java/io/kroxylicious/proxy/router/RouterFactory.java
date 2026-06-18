/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.UnknownNullness;

/**
 * A pluggable source of {@link Router} instances.
 *
 * <p>RouterFactories are {@linkplain java.util.ServiceLoader service}
 * implementations called by the proxy
 * runtime to create router instances.</p>
 *
 * <p>The proxy runtime guarantees that:</p>
 * <ol>
 *   <li>instances will be {@linkplain #initialize initialized} before
 *       any attempt to {@linkplain #createRouter create} router instances,</li>
 *   <li>instances will eventually be {@linkplain #close closed} if and only
 *       if they were successfully initialized,</li>
 *   <li>no attempts to create router instances will be made once a
 *       {@code RouterFactory} instance is closed,</li>
 *   <li>instances will be initialized and closed on the same thread.</li>
 * </ol>
 *
 * <p><strong>Per-connection router instances:</strong>
 * {@link #createRouter} is called once per client connection, so each
 * connection gets its own {@link Router} instance. Any state that must
 * survive a client disconnect and reconnect (e.g. producer ID mappings)
 * must be stored in the shared initialisation data {@code I}, not in
 * the {@code Router} instance itself.</p>
 *
 * @param <C> the configuration type. Use {@link Void} if not configurable.
 * @param <I> the initialisation data type
 */
public interface RouterFactory<C, I> {

    /**
     * Initializes the factory with the given configuration.
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
