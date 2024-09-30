/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import javax.annotation.concurrent.ThreadSafe;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Service interface for KMSs
 * @param <C> The config type
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
@ThreadSafe
public interface KmsService<C, K, E> extends AutoCloseable {

    /**
     * Initialises the service.  This method must be invoked exactly once
     * before {@link #buildKms()} is called.
     *
     * @param config KMS service configuration
     */
    void initialize(C config);

    /**
     * Builds a KMS service.
     * {@link #initialize(C)} must have been called before this method is invoked.
     *
     * @return the KMS.
     * @throws IllegalStateException if the KMS Service has not been initialised or the KMS service is closed.
     */
    @NonNull
    Kms<K, E> buildKms() throws IllegalStateException;

    /**
     * Closes the service. Once the service is closed, the building of new {@link Kms} or the
     * continued use of previously built {@link Kms}s is not allowed. The affect using a {@link Kms}
     * after the service that created it is closed is undefined.
     * <br/>
     * Implementations of this method must be idempotent.
     * <br/>
     * Close implementations must tolerate the closing of service that has not been initialized or
     * one for which initialization did not fully complete without further exception.
     */
    default void close() {
    }
}
