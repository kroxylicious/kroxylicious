/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Support creating an instance of a service, optionally providing it with configuration.
 *
 * @param <S> the service type
 * @param <C> the type of config provided to the service, or {@link Void} for config-less service implementations.
 * @param <X> the context type
 */
public interface Contributor<S, C, X extends Context<C>> {

    /**
     * The concrete type of the service this Contributor can instantiate
     *
     * @return type of the service this Contributor offers.
     */
    @NonNull
    Class<? extends S> getServiceType();

    /**
     * The type of config expected by the service.
     * <br/>
     * The type must have a constructor annotated with the JsonCreator annotation.
     * If the service has no configuration, return {@link Void} instead.
     *
     * @return type of config expected by the service.
     */
    @NonNull
    Class<C> getConfigType();

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
    S createInstance(X context);

}
