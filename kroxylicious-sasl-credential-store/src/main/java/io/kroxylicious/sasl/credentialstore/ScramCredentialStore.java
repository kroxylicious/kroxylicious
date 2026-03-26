/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore;

import java.util.concurrent.CompletionStage;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Store for SCRAM credentials used in SASL authentication.
 * <p>
 * Implementations provide asynchronous lookup of SCRAM credentials by username.
 * The store may be backed by local files, databases, LDAP directories, or remote
 * identity providers.
 * </p>
 * <p>
 * Implementations must support:
 * </p>
 * <ul>
 *     <li><strong>Asynchronous operation</strong> - All methods return {@link CompletionStage}
 *         and must not block</li>
 *     <li><strong>Dynamic updates</strong> - Credentials may be added, removed, or modified
 *         out-of-band while the store is in use</li>
 *     <li><strong>Error signalling</strong> - Service failures must be signalled via
 *         exceptional completion with appropriate exceptions</li>
 *     <li><strong>Thread safety</strong> - Methods may be called concurrently from
 *         multiple threads</li>
 * </ul>
 *
 * <h2>Error Handling</h2>
 * <p>
 * The returned {@link CompletionStage} may complete exceptionally with:
 * </p>
 * <ul>
 *     <li>{@link CredentialServiceUnavailableException} - Backing service is unavailable</li>
 *     <li>{@link CredentialServiceTimeoutException} - Lookup operation timed out</li>
 *     <li>{@link CredentialLookupException} - Other lookup failures</li>
 * </ul>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * ScramCredentialStore store = ...;
 * CompletionStage<ScramCredential> stage = store.lookupCredential("alice");
 * stage.thenAccept(credential -> {
 *     if (credential != null) {
 *         // Authenticate using credential
 *     } else {
 *         // User not found
 *     }
 * }).exceptionally(error -> {
 *     // Handle service failure
 *     return null;
 * });
 * }</pre>
 */
public interface ScramCredentialStore {

    /**
     * Look up SCRAM credentials for a username.
     * <p>
     * The returned {@link CompletionStage} completes with:
     * </p>
     * <ul>
     *     <li>A {@link ScramCredential} if the user exists and has SCRAM credentials</li>
     *     <li>{@code null} if the user does not exist</li>
     *     <li>Exceptional completion if the credential service is unavailable or times out</li>
     * </ul>
     *
     * @param username the username to look up (never null)
     * @return a completion stage that will complete with the credential or null
     * @throws NullPointerException if username is null
     */
    @NonNull
    CompletionStage<ScramCredential> lookupCredential(@NonNull String username);
}
