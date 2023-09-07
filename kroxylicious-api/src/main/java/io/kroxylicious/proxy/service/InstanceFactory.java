/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import io.kroxylicious.proxy.config.BaseConfig;

/**
 * Factory for creating new instances of T and supplies details about the configuration
 * class that should be populated by the Kroxylicious framework when creating an instance.
 * @param <T> The type produced by the factory
 */
public interface InstanceFactory<T> {

    /**
     * Gets the concrete type of the configuration required by this service instance.
     * @return type
     */
    Class<? extends BaseConfig> getConfigClass();

    /**
     * Creates an instance
     * @param config service configuration which may be null if the service instance does not accept configuration
     * @return instance
     */
    T getInstance(BaseConfig config);

}
