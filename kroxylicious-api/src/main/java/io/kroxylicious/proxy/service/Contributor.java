/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.proxy.config.BaseConfig;

/**
 * Support loading an Instance of a service, optionally providing it with configuration obtained
 * from the Kroxylicious configuration file.
 *
 * @param <S> the service type
 * @param <C> the context type
 */
public interface Contributor<S, C extends Context> {
    ConfigurationDefinition NO_CONFIGURATION = new ConfigurationDefinition(BaseConfig.class, false);

    /**
     * Identifies the type name this contributor offers.
     * @return typeName
     */
    @NonNull
    String getTypeName();

    /**
     * Defines the configuration requirements of this contributor.
     * @return the ConfigurationDefinition
     */
    @NonNull
    default ConfigurationDefinition getConfigDefinition() {
        return NO_CONFIGURATION;
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
