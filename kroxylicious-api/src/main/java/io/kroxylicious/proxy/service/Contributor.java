/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import io.kroxylicious.proxy.config.BaseConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Support creating an instance of a service, optionally providing it with configuration.
 *
 * @param <S> the service type
 * @param <C> the context type
 */
public interface Contributor<S, B extends BaseConfig, C extends Context<B>> {

    /**
     * Identifies the type name this contributor offers.
     * @return typeName
     */
    @NonNull
    String getTypeName();

    @NonNull
    Class<B> getConfigType();

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
    S getInstance(C context);

}
