/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.resources;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.runner.ThrowableRunner;

public class Resource<T extends HasMetadata> {
    ThrowableRunner throwableRunner;
    T resource;

    public Resource(ThrowableRunner throwableRunner, T resource) {
        this.throwableRunner = throwableRunner;
        this.resource = resource;
    }

    public Resource(ThrowableRunner throwableRunner) {
        this.throwableRunner = throwableRunner;
    }

    public ThrowableRunner getThrowableRunner() {
        return throwableRunner;
    }

    public T getResource() {
        return resource;
    }
}
