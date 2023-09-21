/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Support loading an Instance of a service, optionally providing it with configuration obtained
 * from the Kroxylicious configuration file.
 *
 * @param <T> the service type
 * @param <S> the context type
 */
public interface Contributor<T, S extends Context> {

    /**
     * Identifies the type name this contributor offers.
     * @return typeName
     */
    @NonNull
    String getTypeName();

    /**
     * Defines the configuration requirements of this contributor for the given short name.
     * @return the ConfigurationDefinition for the short name
     */
    ConfigurationDefinition getConfigDefinition();

    /**
     * Creates an instance of the service.
     *
     * @param context   context containing service configuration which may be null if the service instance does not accept configuration.
     * @return the service instance, or null if this contributor does not offer this short name.
     */
    T getInstance(S context);

}
