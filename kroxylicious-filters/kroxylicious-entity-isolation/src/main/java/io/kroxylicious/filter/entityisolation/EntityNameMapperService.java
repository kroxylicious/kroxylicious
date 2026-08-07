/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

/**
 * A pluggable service responsible for building the {@link EntityNameMapper} used by the
 * {@link EntityIsolationFilter} to map entity names between the downstream and upstream.
 *
 * @param <C> the service configuration type
 */
public interface EntityNameMapperService<C> {

    /**
     * Initialises the service.  This method must be invoked exactly once
     * before {@link #build()} is called.
     *
     * @param config service configuration
     */
    void initialize(C config);

    /**
     * Builds a mapper service.
     * {@link #initialize(C)} must have been called before this method is invoked.
     *
     * @return the mapper.
     * @throws IllegalStateException if the mapper service has not been initialised.
     */
    EntityNameMapper build() throws IllegalStateException;
}
