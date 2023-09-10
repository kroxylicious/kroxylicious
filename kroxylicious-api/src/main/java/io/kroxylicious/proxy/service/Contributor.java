/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import java.util.Optional;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Support loading an InstanceFactory of a service
 *
 * @param <T> the service type
 */
public interface Contributor<T> {

    /**
     * Gets an optional SpecificContributor for this service short name.
     * @param shortName service short name
     *
     * @return optional containing specific contributor, or empty if this contributor does not offer this short name.
     */
    @NonNull
    Optional<SpecificContributor<T>> getSpecificContributor(String shortName);

}
