/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

/**
 * <p>A pluggable source of {@link Filter} instances.</p>
 * <p>FilterFactories are:</p>
 * <ul>
 * <li>{@linkplain java.util.ServiceLoader service} implementations provided by filter authors</li>
 * <li>called by the proxy runtime to {@linkplain #createFilter(FilterFactoryContext, Object) create} filter instances</li>
 * </ul>
 *
 * <p>The proxy runtime guarantees that:</p>
 * <ol>
 *     <li>instances will be {@linkplain  #initialize(FilterFactoryContext, Object) initialized} before any attempt to {@linkplain  #createFilter(FilterFactoryContext, Object) create} filter instances,</li>
 *     <li>instances will eventually be {@linkplain #close(Object)} closed} if and only if they were successfully initialized,</li>
 *     <li>no attempts to create filter instances will be made once a {@code FilterFactory} instance is closed,</li>
 *     <li>instances will be initialized and closed on the same thread.</li>
 * </ol>
 * <p>Filter instance creation can happen on a different thread than initialization or cleanup.
 * It is suggested to pass state using via the return value from {@link #createFilter(FilterFactoryContext, Object)} rather than
 * relying on synchronization within a filter factory implementation.</p>
 *
 * @param <C> the type of configuration used to create the {@code Filter}. Use {@link Void} if the {@code Filter} is not configurable.
 * @param <I> The type of the initialization data
 */
public interface FilterFactory<C, I> {

    /**
     * <p>Initializes the factory with the specified configuration.</p>
     *
     * <p>This method is guaranteed to be called at most once for each filter configuration and before any call to
     * {@link #createFilter(FilterFactoryContext, Object)}.
     * This method may provide extra semantic validation of the config,
     * and returns some object (which may be the config, or some other object) which will be passed to {@link #createFilter(FilterFactoryContext, Object)}.
     *
     * @param context context
     * @param config configuration
     * @return A configuration state object, specific to the given {@code config}, which will be passed to the other methods of this interface.
     * @throws PluginConfigurationException when the configuration is invalid
     */
    I initialize(FilterFactoryContext context, C config) throws PluginConfigurationException;

    /**
     * Creates an instance of the Filter.
     *
     * <p>This can be called on a different thread from {@link #initialize(FilterFactoryContext, Object)} and {@link #close(I)}.
     * Implementors should either use the {@code initializationData} to pass state from {@link #initialize(FilterFactoryContext, Object)}
     * or use appropriate synchronization.</p>
     *
     * @param context The runtime context for the filter's creation.
     * @param initializationData The initialization data that was returned from {@link #initialize(FilterFactoryContext, Object)}.
     * @return the Filter instance.
     */
    Filter createFilter(FilterFactoryContext context, I initializationData);

    /**
     * Called by the runtime to release any resources associated with the given {@code initializationData}.
     * This is guaranteed to eventually be called for each successful call to {@link #initialize(FilterFactoryContext, Object)}.
     * Once this method has been called {@link #createFilter(FilterFactoryContext, Object)} won't be called again.
     * @param initializationData The initialization data that was returned from {@link #initialize(FilterFactoryContext, Object)}.
     */
    default void close(I initializationData) {
    }
}
