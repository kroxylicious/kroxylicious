/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.service.Contributor;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * FilterContributor is a pluggable source of Kroxylicious filter implementations.
 * @see Contributor
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
     * If requiresConfiguration returns true and getConfigType returns a non-Void
     * type, then the framework will enforce that the config object passed to createInstance
     * within the context is non-null. If set to false then null configuration can be passed
     * in the context.
     * @return true if the configuration must be non-null, false if it is allowed to be null
     */
    @NonNull
    default boolean requiresConfiguration() {
        return false;
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
