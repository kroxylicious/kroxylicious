/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2.api;

/**
 * A base type for a plugin that can be loaded from a service loader
 * @param <S> The service type
 */
public abstract class Plugin<S> {
    private final Class<S> serviceClass;

    public Plugin(Class<S> serviceClass) {
        this.serviceClass = serviceClass;
    }

    public Class<S> serviceClass() {
        return serviceClass;
    }
}

