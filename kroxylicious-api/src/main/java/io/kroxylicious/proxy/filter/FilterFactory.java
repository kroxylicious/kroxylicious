/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.InvalidConfigurationException;
import io.kroxylicious.proxy.service.Contributor;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * FilterContributor is a pluggable source of Kroxylicious filter implementations.
 * @see Contributor
 * @param <B> the configuration type for the Filter (use {@link Void} if the Filter is not configurable)
 */
public interface FilterFactory<B> {

    /**
     * The concrete type of the service this Contributor can instantiate
     *
     * @return type of the service this Contributor offers.
     */
    @NonNull
    Class<? extends Filter> getServiceType();

    /**
     * The type of config expected by the service.
     * <br/>
     * The type must have a constructor annotated with the JsonCreator annotation.
     * If the service has no configuration, return {@link Void} instead.
     *
     * @return type of config expected by the service.
     */
    @NonNull
    Class<B> getConfigType();

    /**
     * Validate the configuration. By default, the configuration is considered invalid if
     * the config type is not {@link Void} and the configuration is null.
     * @param config configuration
     * @throws InvalidConfigurationException when the configuration is invalid
     */
    default void validateConfiguration(B config) {
        boolean requiresConfiguration = getConfigType() != Void.class;
        if (requiresConfiguration && config == null) {
            throw new InvalidConfigurationException(getServiceType().getSimpleName() + " requires configuration, but config object is null");
        }
    }

    /**
     * Creates an instance of the service.
     *
     * @param context   context containing service configuration which may be null if the service instance does not accept configuration.
     * @return the service instance.
     */
    @NonNull
    Filter createInstance(FilterConstructContext<B> context);

}
