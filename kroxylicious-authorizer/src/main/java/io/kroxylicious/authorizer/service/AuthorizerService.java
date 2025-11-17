/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service;

import javax.annotation.concurrent.ThreadSafe;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.UnknownNullness;

/**
 * Service interface for {@link Authorizer Authorizers}.
 * @param <C> The config type
 */
@ThreadSafe
public interface AuthorizerService<C> {

    /**
     * Initialises the service.  This method must be invoked exactly once
     * before {@link #build()} is called.
     *
     * @param config Service configuration
     */
    void initialize(@UnknownNullness C config);

    /**
     * Builds an Authorizer service.
     * {@link #initialize(C)} must have been called before this method is invoked.
     *
     * @return the authorizer
     * @throws IllegalStateException if the service has not been initialised or the service is closed.
     */
    @NonNull
    Authorizer build() throws IllegalStateException;

    /**
     * Closes the service. Once the service is closed, the building of new instances or the
     * continued use of previously built instances is not allowed. The effect using an instance
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
