/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * FilterFactory is a pluggable source of {@link Filter} instances.
 * @param <F> the {@code Filter} type.
 * @param <C> the type of configuration used to create the {@code Filter}. Use {@link Void} if the {@code Filter} is not configurable.
 */
public abstract class FilterFactory<F extends Filter, C> {

    private final Class<C> configType;
    private final Class<F> filterType;

    /**
     * Constructor
     * @param configType The concrete class of {@code Filter} this factory {@linkplain #createFilter(FilterCreationContext, C) creates}.
     * @param filterType The type of configuration this factory requires.
     * Use {@code Void.class} if the factory does not support configuration.
     */
    public FilterFactory(Class<C> configType, Class<F> filterType) {
        this.configType = Objects.requireNonNull(configType);
        this.filterType = Objects.requireNonNull(filterType);
    }

    /**
     * The type of filter.
     * @return The concrete class of {@code Filter} this factory {@linkplain #createFilter(FilterCreationContext, C) creates}.
     */
    public final Class<F> filterType() {
        return filterType;
    }

    /**
     * The type of configuration used to create the {@code Filter}.
     * <br/>
     * The type must be deserializable with Jackson
     * If the Filter has no configuration, return {@link Void} instead.
     *
     * @return type of config expected by the Filter.
     */
    public final Class<C> configType() {
        return configType;
    }

    /**
     * Validates the configuration.
     * By default, the configuration is considered valid if configuration is non-null
     * or the {@link #configType()} is {@link Void}.
     * In other words, configuration is required unless the factory doesn't support configuration at all.
     * This method should be overridden to provide extra semantic validation of the config,
     * checking for required configuration properties or bounds checking numerical configuration properties.
     * @param config configuration
     * @throws InvalidFilterConfigurationException when the configuration is invalid
     */
    public void validateConfiguration(C config) {
        boolean requiresConfiguration = configType() != Void.class;
        if (requiresConfiguration && config == null) {
            throw new InvalidFilterConfigurationException(filterType().getSimpleName() + " requires configuration, but config object is null");
        }
    }

    /**
     * Creates an instance of the Filter.
     *
     * @param context The runtime context for the filter's creation.
     * @param configuration configuration, which will be null if no configuration was provided or the {@link #configType()} is {@code Void}.
     * @return the Filter instance.
     */
    @NonNull
    public abstract F createFilter(FilterCreationContext context, C configuration);

}
