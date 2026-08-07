/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.util.concurrent.CompletionStage;

/**
 * Mechanism-specific transition function for the {@link io.kroxylicious.filter.sasl.termination.State State} state machine.
 * <p>
 * When the filter is in the {@link io.kroxylicious.filter.sasl.termination.State.RequiringAuthenticate RequiringAuthenticate}
 * state, it delegates each {@code SaslAuthenticate} request to the {@code MechanismStateMachine} for the
 * negotiated mechanism. The returned {@link RoundResult} tells the filter which state transition to make:
 * {@linkplain RoundResult.Challenge continue authenticating},
 * {@linkplain RoundResult.Success transition to authenticated}, or
 * {@linkplain RoundResult.Failure fail}.
 * </p>
 * <p>
 * Implementations may maintain internal state across rounds (e.g.&nbsp;SCRAM tracks
 * nonces and proofs), but this state is private to the mechanism and invisible to the
 * filter-level state machine.
 * </p>
 *
 * <h2>Multi-Round Authentication</h2>
 * <p>
 * Some mechanisms (like SCRAM) require multiple rounds of exchange. The state machine
 * must return {@link RoundResult.Challenge} until the final round,
 * then {@link RoundResult.Success} or {@link RoundResult.Failure}.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * Implementations are NOT required to be thread-safe. Each instance is used
 * for a single connection and is accessed only from that connection's event loop thread.
 * </p>
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *     <li>Instance created per authentication session</li>
 *     <li>{@link #evaluateRound} called one or more times</li>
 *     <li>{@link #dispose()} called to clean up resources</li>
 * </ol>
 */
interface MechanismStateMachine {

    /**
     * Get the IANA-registered mechanism name.
     *
     * @return the mechanism name (e.g., "SCRAM-SHA-256")
     */
    String mechanismName();

    /**
     * Maximum size in bytes of the {@code authBytes} field in a
     * {@code SaslAuthenticateRequest} that this mechanism will accept.
     * Payloads exceeding this limit are rejected before processing.
     *
     * @return the maximum auth bytes size
     */
    int maxAuthBytes();

    /**
     * Process one authentication round and return the resulting state transition.
     * <p>
     * This method is called for each authenticate request from the client.
     * For multi-round mechanisms, it will be called multiple times with different
     * request bytes.
     * </p>
     * <p>
     * The returned {@link CompletionStage} must not block. Any I/O (credential
     * lookups, token validation) should be asynchronous. Mechanism-specific
     * resources (credential stores, callback handlers) are injected at
     * construction time via the factory.
     * </p>
     * <p>
     * <strong>Timing side-channel mitigation:</strong> The filter applies a fixed
     * delay to each round to prevent timing-based username enumeration. For this
     * mitigation to be effective, multi-round implementations that detect an authentication
     * failure in an early round (e.g. unknown user) must not fail fast. Instead,
     * they should continue to return {@link RoundResult.Challenge}
     * responses for intermediate rounds and defer the
     * {@link RoundResult.Failure} until the final round, so that
     * the number of rounds is indistinguishable from a legitimate exchange.
     * </p>
     *
     * @param authBytes the SASL authentication bytes from the client
     * @return a completion stage that completes with the authentication result
     */
    CompletionStage<RoundResult> evaluateRound(byte[] authBytes);

    /**
     * Dispose of resources used by this state machine.
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
