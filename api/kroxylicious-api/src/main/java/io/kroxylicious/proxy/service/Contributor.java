/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import io.kroxylicious.proxy.config.BaseConfig;

/**
 * Support loading an Instance of a service, optionally providing it with configuration obtained
 * from the Kroxylicious configuration file.
 *
 * @param <T> the service type
 */
public interface Contributor<T> {

    /**
     * Gets the concrete type of the configuration required by this service instance.
     * @param shortName service short name
     *
     * @return class of a concrete type or null, if this service instance does not accept configuration.
     */
    Class<? extends BaseConfig> getConfigType(String shortName);

    /**
     * Creates an instance of the service.
     *
     * @param shortName service short name
     * @param endpointProvider endpoint provider
     * @param config service configuration which may be null if the service instance does not accept configuration.
     * @return the service instance
     */
    T getInstance(String shortName, ClusterEndpointProvider endpointProvider, BaseConfig config);
}
