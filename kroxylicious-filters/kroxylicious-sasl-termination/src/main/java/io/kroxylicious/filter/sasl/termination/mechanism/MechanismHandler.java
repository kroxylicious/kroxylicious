/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.util.concurrent.CompletionStage;

import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;

/**
 * Handler for a specific SASL mechanism.
 * <p>
 * Handles the authentication exchange for a single mechanism (e.g., SCRAM-SHA-256).
 * Implementations are responsible for:
 * </p>
 * <ul>
 *     <li>Processing SASL authenticate request bytes</li>
 *     <li>Looking up credentials asynchronously</li>
 *     <li>Generating response bytes</li>
 *     <li>Determining authentication outcome (challenge/success/failure)</li>
 *     <li>Cleaning up resources when done</li>
 * </ul>
 *
 * <h2>Multi-Round Authentication</h2>
 * <p>
 * Some mechanisms (like SCRAM) require multiple rounds of exchange. The handler
 * must return {@link AuthenticationResult.Outcome#CHALLENGE} until the final round,
 * then {@link AuthenticationResult.Outcome#SUCCESS} or {@link AuthenticationResult.Outcome#FAILURE}.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * Handlers are NOT required to be thread-safe. Each handler instance is used
 * for a single connection and is accessed only from that connection's event loop thread.
 * </p>
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *     <li>Handler created via {@link MechanismHandlerFactory}</li>
 *     <li>{@link #handleAuthenticate} called one or more times</li>
 *     <li>{@link #dispose()} called to clean up resources</li>
 * </ol>
 */
public interface MechanismHandler {

    /**
     * Get the IANA-registered mechanism name.
     *
     * @return the mechanism name (e.g., "SCRAM-SHA-256")
     */
    String mechanismName();

    /**
     * Handle a SASL authenticate request.
     * <p>
     * This method is called for each authenticate request from the client.
     * For multi-round mechanisms, it will be called multiple times with different
     * request bytes.
     * </p>
     * <p>
     * The returned {@link CompletionStage} must not block. Credential lookups should
     * be asynchronous. Use the provided {@link ScramCredentialStore} for async access.
     * </p>
     *
     * @param authBytes the SASL authentication bytes from the client
     * @param credentialStore the credential store for looking up user credentials
     * @return a completion stage that completes with the authentication result
     */
    CompletionStage<AuthenticationResult> handleAuthenticate(
                                                             byte[] authBytes,
                                                             ScramCredentialStore credentialStore);

    /**
     * Dispose of resources used by this handler.
     * <p>
     * Called when authentication completes (successfully or not) or when the
     * connection is closed. Must be idempotent.
     * </p>
     * <p>
     * Implementations should dispose of any SASL server instances, clear sensitive
     * data from memory, etc.
     * </p>
     */
    void dispose();
}
