/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.UnknownNullness;

/**
 * <p>A pluggable source of {@link Filter} instances.</p>
 * <p>FilterFactories are:</p>
 * <ul>
 * <li>{@linkplain java.util.ServiceLoader service} implementations provided by filter authors</li>
 * <li>called by the proxy runtime to {@linkplain #createFilter(FilterFactoryContext, Object) create} filter instances</li>
 * </ul>
 *
 * <h2>Lifecycle scope</h2>
 * <p>{@code FilterFactory} instances are scoped <strong>per virtual cluster</strong>: each virtual
 * cluster that references a filter definition gets its own {@code FilterFactory} instance with its
 * own {@link #initialize(FilterFactoryContext, Object) initialize}/{@link #close(Object) close}
 * pair. The same filter type used by two virtual clusters produces two independent instances with
 * independent initialization data — there is no cross-virtual-cluster sharing. Within a single
 * virtual cluster, a filter definition referenced multiple times in the chain (e.g. an audit filter
 * applied before and after a transformation) is initialized once and its initialization data is
 * shared across all positions in that virtual cluster's chain.</p>
 *
 * <p>Per virtual cluster, the proxy runtime guarantees that:</p>
 * <ol>
 *     <li>instances will be {@linkplain  #initialize(FilterFactoryContext, Object) initialized} before any attempt to {@linkplain  #createFilter(FilterFactoryContext, Object) create} filter instances,</li>
 *     <li>instances will eventually be {@linkplain #close(Object)} closed} if and only if they were successfully initialized,</li>
 *     <li>no attempts to create filter instances will be made once a {@code FilterFactory} instance is closed,</li>
 *     <li>{@code initialize} and {@code close} are never invoked on a Netty event loop thread — blocking work (e.g. closing an HTTP/KMS client) is safe in either method.</li>
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
     * <p>This method is guaranteed to be called at most once <em>per virtual cluster</em> for each
     * filter definition referenced by that virtual cluster, and before any call to
     * {@link #createFilter(FilterFactoryContext, Object)} on that virtual cluster. Note that
     * because {@code FilterFactory} instances are per-virtual-cluster (see the class javadoc),
     * the same filter type used by multiple virtual clusters will see one {@code initialize} call
     * per virtual cluster — each on a different {@code FilterFactory} instance with its own
     * initialization data.</p>
     *
     * <p>This method may provide extra semantic validation of the config,
     * and returns some object (which may be the config, or some other object) which will be passed to {@link #createFilter(FilterFactoryContext, Object)}.</p>
     *
     * @param context context
     * @param config configuration
     * @return A configuration state object, specific to the given {@code config}, which will be passed to the other methods of this interface.
     * @throws PluginConfigurationException when the configuration is invalid
     */
    @UnknownNullness
    I initialize(FilterFactoryContext context, @UnknownNullness C config) throws PluginConfigurationException;

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
    Filter createFilter(FilterFactoryContext context, @UnknownNullness I initializationData);

    /**
     * Called by the runtime to release any resources associated with the given {@code initializationData}.
     * This is guaranteed to eventually be called for each successful call to {@link #initialize(FilterFactoryContext, Object)}.
     * Once this method has been called {@link #createFilter(FilterFactoryContext, Object)} won't be called again.
     * @param initializationData The initialization data that was returned from {@link #initialize(FilterFactoryContext, Object)}.
     */
    default void close(@UnknownNullness I initializationData) {
    }
}
