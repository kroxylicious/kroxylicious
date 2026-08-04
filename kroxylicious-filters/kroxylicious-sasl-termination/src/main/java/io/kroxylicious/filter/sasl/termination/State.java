/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Instant;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * States of the {@link SaslTerminationFilter} state machine.
 * <p>
 * The filter authenticates clients before allowing other Kafka requests.
 * Transitions between states are driven by Kafka protocol messages
 * ({@code SaslHandshake} and {@code SaslAuthenticate} requests).
 * While in {@link RequiringAuthenticate}, the mechanism-specific logic is
 * delegated to a {@link MechanismStateMachine}, whose {@link RoundResult}
 * determines the next state transition.
 * </p>
 *
 * <h2>State Diagram</h2>
 * <pre><code>
 * START ──→ RequiringHandshake
 *                   │
 *                   ↓
 *          RequiringAuthenticate ←────╮
 *                   │                 │
 *                   ├─ (multi-round) ─╯
 *                   │
 *                   ├──→ Authenticated ──→ (reauth) ──→ RequiringAuthenticate
 *                   │         │
 *                   │         └──→ (expired + non-SASL request) ──→ reject &amp; close
 *                   │
 *                   └──→ Failed (terminal, failure)
 * </code></pre>
 *
 * <h2>State Descriptions</h2>
 * <ul>
 *     <li><strong>RequiringHandshake</strong> - Initial state. A SASL handshake request
 *         is required to negotiate the mechanism.</li>
 *     <li><strong>RequiringAuthenticate</strong> - Handshake complete. Waiting for
 *         authentication request(s). May loop for multi-round mechanisms like SCRAM.</li>
 *     <li><strong>Authenticated</strong> - Terminal state. Authentication succeeded.
 *         Client may now send other Kafka requests.</li>
 *     <li><strong>Failed</strong> - Terminal state. Authentication failed. Connection
 *         should be closed.</li>
 * </ul>
 *
 * <h2>Security Barrier</h2>
 * <p>
 * Only {@link Authenticated} state allows non-SASL Kafka requests. All other states
 * must reject such requests with {@code SASL_AUTHENTICATION_FAILED} and close the connection.
 * </p>
 * <p>
 * {@code API_VERSIONS} requests are always allowed (clients need to discover capabilities).
 * </p>
 */
sealed interface State permits State.RequiringHandshake, State.RequiringAuthenticate, State.Authenticated, State.Failed {

    /**
     * Create the initial state.
     *
     * @return the requiring handshake state
     */
    static RequiringHandshake start() {
        return new RequiringHandshake();
    }

    /**
     * Check if authentication has completed successfully.
     *
     * @return true if in the Authenticated state
     */
    default boolean isAuthenticated() {
        return this instanceof Authenticated;
    }

    /**
     * Check if authentication has failed.
     *
     * @return true if in the Failed state
     */
    default boolean isFailed() {
        return this instanceof Failed;
    }

    /**
     * Check if in a terminal state (success or failure).
     *
     * @return true if authenticated or failed
     */
    default boolean isTerminal() {
        return isFailed();
    }

    /**
     * Initial state - waiting for SASL handshake request.
     */
    final class RequiringHandshake implements State {

        private RequiringHandshake() {
        }

        /**
         * Transition to the next state after receiving handshake request.
         *
         * @param mechanismStateMachine the state machine for the negotiated mechanism
         * @param authStartNanos the {@link System#nanoTime()} when authentication started
         * @return the requiring authenticate state
         */
        public RequiringAuthenticate nextState(MechanismStateMachine mechanismStateMachine, long authStartNanos) {
            return new RequiringAuthenticate(mechanismStateMachine, authStartNanos);
        }

        @Override
        public String toString() {
            return "RequiringHandshake";
        }
    }

    /**
     * Waiting for SASL authenticate request.
     * <p>
     * May loop back to this state for multi-round mechanisms (SCRAM).
     * </p>
     */
    final class RequiringAuthenticate implements State {

        private final MechanismStateMachine mechanismStateMachine;
        private final long authStartNanos;

        private RequiringAuthenticate(MechanismStateMachine mechanismStateMachine, long authStartNanos) {
            this.mechanismStateMachine = mechanismStateMachine;
            this.authStartNanos = authStartNanos;
        }

        /**
         * Get the mechanism state machine for this authentication session.
         *
         * @return the mechanism state machine
         */
        public MechanismStateMachine mechanismStateMachine() {
            return mechanismStateMachine;
        }

        /**
         * Get the {@link System#nanoTime()} when authentication started.
         *
         * @return the auth start time in nanos
         */
        public long authStartNanos() {
            return authStartNanos;
        }

        /**
         * Transition to the next state after a challenge round.
         * <p>
         * Used for multi-round mechanisms that require additional exchanges.
         * </p>
         *
         * @return a new requiring authenticate state (loop)
         */
        public RequiringAuthenticate nextStateChallenge() {
            return this; // Stay in same state for next round
        }

        /**
         * Transition to authenticated state after successful authentication.
         *
         * @param authorizationId the authenticated user's authorization ID
         * @param mechanismName the name of the mechanism used for authentication
         * @param sessionExpiry when the session expires, or null if no expiry
         * @return the authenticated state
         */
        public Authenticated nextStateSuccess(String authorizationId, String mechanismName, @Nullable Instant sessionExpiry) {
            return new Authenticated(authorizationId, mechanismName, sessionExpiry);
        }

        /**
         * Transition to failed state after authentication failure.
         *
         * @param errorMessage the error message
         * @return the failed state
         */
        public Failed nextStateFailure(String errorMessage) {
            return new Failed(errorMessage);
        }

        @Override
        public String toString() {
            return "RequiringAuthenticate{mechanism=" + mechanismStateMachine.mechanismName() + "}";
        }
    }

    /**
     * Authentication succeeded. Allows reauthentication (KIP-368) by
     * transitioning back to {@link RequiringAuthenticate}.
     */
    final class Authenticated implements State {

        private final String authorizationId;
        private final String mechanismName;

        @Nullable
        private final Instant sessionExpiry;

        private Authenticated(String authorizationId, String mechanismName, @Nullable Instant sessionExpiry) {
            this.authorizationId = authorizationId;
            this.mechanismName = mechanismName;
            this.sessionExpiry = sessionExpiry;
        }

        /**
         * Get the authenticated user's authorization ID.
         *
         * @return the authorization ID
         */
        public String authorizationId() {
            return authorizationId;
        }

        /**
         * Get the mechanism used for authentication.
         *
         * @return the mechanism name
         */
        public String mechanismName() {
            return mechanismName;
        }

        /**
         * Get the session expiry time.
         *
         * @return the session expiry instant, or null if no expiry
         */
        @Nullable
        public Instant sessionExpiry() {
            return sessionExpiry;
        }

        /**
         * Transition to reauthentication after receiving a new SASL handshake.
         *
         * @param mechanismStateMachine the state machine for the new authentication session
         * @param authStartNanos the {@link System#nanoTime()} when reauthentication started
         * @return the requiring authenticate state
         */
        public RequiringAuthenticate nextStateReauthenticate(MechanismStateMachine mechanismStateMachine, long authStartNanos) {
            return new RequiringAuthenticate(mechanismStateMachine, authStartNanos);
        }

        @Override
        public String toString() {
            return "Authenticated{user=" + authorizationId + ", sessionExpiry=" + sessionExpiry + "}";
        }
    }

    /**
     * Terminal state - authentication failed.
     */
    final class Failed implements State {

        private final String errorMessage;

        @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "errorMessage is intentionally nullable - null is printed as 'null' in toString()")
        private Failed(@Nullable String errorMessage) {
            this.errorMessage = errorMessage;
        }

        /**
         * Get the error message describing why authentication failed.
         *
         * @return the error message (may be null)
         */
        @Nullable
        public String errorMessage() {
            return errorMessage;
        }

        @Override
        public String toString() {
            return "Failed{error=" + errorMessage + "}";
        }
    }
}
