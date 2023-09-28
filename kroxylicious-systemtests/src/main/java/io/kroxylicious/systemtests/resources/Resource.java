/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.systemtests.runner.ThrowableRunner;

/**
 * The type Resource.
 *
 * @param <T>  the type parameter
 */
public class Resource<T extends HasMetadata> {
    /**
     * The Throwable runner.
     */
    ThrowableRunner throwableRunner;
    /**
     * The Resource.
     */
    T resource;

    /**
     * Instantiates a new Resource.
     *
     * @param throwableRunner the throwable runner
     * @param resource the resource
     */
    public Resource(ThrowableRunner throwableRunner, T resource) {
        this.throwableRunner = throwableRunner;
        this.resource = resource;
    }

    /**
     * Instantiates a new Resource.
     *
     * @param throwableRunner the throwable runner
     */
    public Resource(ThrowableRunner throwableRunner) {
        this.throwableRunner = throwableRunner;
    }

    /**
     * Gets throwable runner.
     *
     * @return the throwable runner
     */
    public ThrowableRunner getThrowableRunner() {
        return throwableRunner;
    }

    /**
     * Gets resource.
     *
     * @return the resource
     */
    public T getResource() {
        return resource;
    }
}
