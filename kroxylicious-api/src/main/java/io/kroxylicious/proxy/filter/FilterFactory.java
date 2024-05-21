/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

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
 *     <li>instances that are successfully initialized will eventually be {@linkplain #close() closed},</li>
 *     <li>no attempts to create filter instances will be made once a {@code FilterFactory} instance is closed.</li>
 * </ol>
 *
 * @param <C> the type of configuration used to create the {@code Filter}. Use {@link Void} if the {@code Filter} is not configurable.
 * @param <I> The type of the initialization data
 */
public interface FilterFactory<C, I> extends AutoCloseable {

    /**
     * Initializes the factory with the specified configuration.
     * This method is guaranteed to be called at most once for each filter configuration and before any call to
     * {@link #createFilter(FilterFactoryContext, Object)}.
     * This method may provide extra semantic validation of the config,
     * and returns some object (which may be the config, or some other object) which will be passed to {@link #createFilter(FilterFactoryContext, Object)}.
     *
     * @param context context
     * @param config configuration
     * @throws PluginConfigurationException when the configuration is invalid
     */
    I initialize(FilterFactoryContext context, C config) throws PluginConfigurationException;

    /**
     * Creates an instance of the Filter.
     *
     * @param context The runtime context for the filter's creation.
     * @param initializationData The result of the call to {@link #initialize(FilterFactoryContext, Object)}
     * @return the Filter instance.
     */
    @NonNull
    Filter createFilter(FilterFactoryContext context, I initializationData);

    /**
     * Called by the runtime to release any resources held by this factory.
     * This is guaranteed to be called if an instance has been {@linkplain #initialize(FilterFactoryContext, Object) initialized}.
     * Once this method has been called {@link #createFilter(FilterFactoryContext, Object)} won't be called again.
     */
    @Override
    default void close() {
    }
}
