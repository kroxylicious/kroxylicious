/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore;

import javax.annotation.concurrent.ThreadSafe;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Service interface for SCRAM credential stores.
 * <p>
 * Implementations provide pluggable sources of SCRAM credentials for SASL authentication.
 * Credential stores may be backed by files (KeyStores), databases, LDAP directories,
 * or external identity providers.
 * </p>
 * <p>
 * The service follows a two-phase lifecycle:
 * </p>
 * <ol>
 *     <li>{@link #initialize(Object)} - Configure the service with type-safe configuration</li>
 *     <li>{@link #buildCredentialStore()} - Build credential store instances (may be called multiple times)</li>
 *     <li>{@link #close()} - Clean up resources when done</li>
 * </ol>
 * <p>
 * Implementations must be thread-safe as they may be called from multiple filter instances concurrently.
 * </p>
 *
 * @param <C> The configuration type
 */
@ThreadSafe
public interface ScramCredentialStoreService<C> extends AutoCloseable {

    /**
     * Initialises the service. This method must be invoked exactly once
     * before {@link #buildCredentialStore()} is called.
     *
     * @param config credential store service configuration
     */
    void initialize(C config);

    /**
     * Builds a credential store.
     * {@link #initialize(Object)} must have been called before this method is invoked.
     *
     * @return the credential store
     * @throws IllegalStateException if the service has not been initialised or the service is closed
     */
    @NonNull
    ScramCredentialStore buildCredentialStore() throws IllegalStateException;

    /**
     * Closes the service. Once the service is closed, the building of new {@link ScramCredentialStore}
     * or the continued use of previously built {@link ScramCredentialStore}s is not allowed. The effect of
     * using a {@link ScramCredentialStore} after the service that created it is closed is undefined.
     * <br/>
     * Implementations of this method must be idempotent.
     * <br/>
     * Close implementations must tolerate the closing of a service that has not been initialised or
     * one for which initialisation did not fully complete without further exception.
     */
    @Override
    default void close() {
    }
}
