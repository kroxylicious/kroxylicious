/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Service interface for KMSs
 * @param <C> The config type
 * @param <I> The initialization type
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
public interface KmsService<C, I, K, E> {

    /**
     * Initialises the service.  This method must be invoked exactly once
     * before {@link #buildKms(Object)} is called.
     *
     * @param config configuration
     * @return initializationData
     */
    default I initialize(C config) {
        return (I) config;
    }

    /**
     * Builds the KMS service.
     *
     * @param initializationData initialization data
     * @return the KMS.
     */
    @NonNull
    Kms<K, E> buildKms(I initializationData);

    /**
     * Closes the service.  The caller must pass the {@code initializationData}
     * returned by the call to {@link #initialize(Object)}.  Implementations
     * of this method must be idempotent.
     *
     * @param initializationData initialization data
     */
    default void close(I initializationData) {
    }
}
