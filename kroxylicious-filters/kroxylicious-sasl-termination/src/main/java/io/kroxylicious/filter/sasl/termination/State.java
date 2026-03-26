/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandler;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * State machine for SASL termination filter.
 * <p>
 * The filter acts as a SASL server, authenticating clients before allowing
 * them to send other Kafka requests. The state machine enforces the SASL
 * protocol flow and provides a security barrier.
 * </p>
 *
 * <h2>State Diagram</h2>
 * <pre><code>
 * START ──→ RequiringHandshake
 *                   │
 *                   ↓
 *          RequiringAuthenticate ←──╮
 *                   │                │
 *                   ├─ (multi-round) ─╯
 *                   │
 *                   ├──→ Authenticated (terminal, success)
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
    @NonNull
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
        return isAuthenticated() || isFailed();
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
         * @param mechanismHandler the handler for the negotiated mechanism
         * @return the requiring authenticate state
         */
        @NonNull
        public RequiringAuthenticate nextState(@NonNull MechanismHandler mechanismHandler) {
            return new RequiringAuthenticate(mechanismHandler);
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

        private final MechanismHandler mechanismHandler;

        private RequiringAuthenticate(@NonNull MechanismHandler mechanismHandler) {
            this.mechanismHandler = mechanismHandler;
        }

        /**
         * Get the mechanism handler for this authentication session.
         *
         * @return the mechanism handler
         */
        @NonNull
        public MechanismHandler mechanismHandler() {
            return mechanismHandler;
        }

        /**
         * Transition to the next state after a challenge round.
         * <p>
         * Used for multi-round mechanisms that require additional exchanges.
         * </p>
         *
         * @return a new requiring authenticate state (loop)
         */
        @NonNull
        public RequiringAuthenticate nextStateChallenge() {
            return this; // Stay in same state for next round
        }

        /**
         * Transition to authenticated state after successful authentication.
         *
         * @param authorizationId the authenticated user's authorization ID
         * @return the authenticated state
         */
        @NonNull
        public Authenticated nextStateSuccess(@NonNull String authorizationId) {
            return new Authenticated(authorizationId);
        }

        /**
         * Transition to failed state after authentication failure.
         *
         * @param errorMessage the error message
         * @return the failed state
         */
        @NonNull
        public Failed nextStateFailure(@NonNull String errorMessage) {
            return new Failed(errorMessage);
        }

        @Override
        public String toString() {
            return "RequiringAuthenticate{mechanism=" + mechanismHandler.mechanismName() + "}";
        }
    }

    /**
     * Terminal state - authentication succeeded.
     */
    final class Authenticated implements State {

        private final String authorizationId;

        private Authenticated(@NonNull String authorizationId) {
            this.authorizationId = authorizationId;
        }

        /**
         * Get the authenticated user's authorization ID.
         *
         * @return the authorization ID
         */
        @NonNull
        public String authorizationId() {
            return authorizationId;
        }

        @Override
        public String toString() {
            return "Authenticated{user=" + authorizationId + "}";
        }
    }

    /**
     * Terminal state - authentication failed.
     */
    final class Failed implements State {

        private final String errorMessage;

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
