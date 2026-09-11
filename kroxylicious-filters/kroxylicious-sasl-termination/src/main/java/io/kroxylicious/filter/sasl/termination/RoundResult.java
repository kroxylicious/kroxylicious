/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Outcome of a single authentication round, encoding the state transition
 * that the filter-level state machine should make.
 * <ul>
 *     <li>{@link Challenge} &mdash; remain in {@link io.kroxylicious.filter.sasl.termination.State.RequiringAuthenticate RequiringAuthenticate}
 *         (another round is needed)</li>
 *     <li>{@link Success} &mdash; transition to {@link io.kroxylicious.filter.sasl.termination.State.Authenticated Authenticated}</li>
 *     <li>{@link Failure} &mdash; transition to {@link io.kroxylicious.filter.sasl.termination.State.Failed Failed}</li>
 * </ul>
 */
sealed interface RoundResult
        permits RoundResult.Challenge, RoundResult.Success, RoundResult.Failure {

    /**
     * The bytes to send in the SASL authenticate response.
     *
     * @return a defensive copy of the response bytes
     */
    byte[] responseBytes();

    /**
     * Create a challenge result.
     *
     * @param responseBytes the challenge bytes
     * @return the round result
     */
    static Challenge challenge(byte[] responseBytes) {
        return new Challenge(responseBytes);
    }

    /**
     * Create a success result with no session expiry (no reauthentication).
     *
     * @param responseBytes the final response bytes
     * @param authorizationId the authenticated user's authorization ID
     * @return the round result
     */
    static Success success(byte[] responseBytes, String authorizationId) {
        return new Success(responseBytes, authorizationId, null);
    }

    /**
     * Create a success result with a session expiry for reauthentication (KIP-368).
     *
     * @param responseBytes the final response bytes
     * @param authorizationId the authenticated user's authorization ID
     * @param sessionExpiry absolute time at which the credential expires, null = no expiry
     * @return the round result
     */
    static Success success(byte[] responseBytes, String authorizationId, @Nullable Instant sessionExpiry) {
        return new Success(responseBytes, authorizationId, sessionExpiry);
    }

    /**
     * Create a failure result.
     *
     * @param responseBytes the error response bytes (may be empty)
     * @param exception the exception describing the authentication failure
     * @return the round result
     */
    static Failure failure(byte[] responseBytes, Exception exception) {
        return new Failure(responseBytes, exception);
    }

    /**
     * Authentication requires another round (e.g. SCRAM).
     * The client must send another authenticate request.
     *
     * @param responseBytes the challenge bytes to send to the client
     */
    @SuppressWarnings("ArrayRecordComponent") // defensive copies, equals and hashCode are overridden
    record Challenge(byte[] responseBytes) implements RoundResult {

        /**
         * Canonical constructor with validation and defensive copy.
         */
        public Challenge {
            Objects.requireNonNull(responseBytes);
            responseBytes = responseBytes.clone();
        }

        @Override
        public byte[] responseBytes() {
            return responseBytes.clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof Challenge(byte[] thatResponseBytes)
                    && Arrays.equals(responseBytes, thatResponseBytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(responseBytes);
        }

        @Override
        public String toString() {
            return "Challenge{}";
        }
    }

    /**
     * Authentication succeeded.
     *
     * @param responseBytes the final response bytes to send to the client
     * @param authorizationId the authenticated user's authorization ID
     * @param sessionExpiry absolute time at which the credential expires (KIP-368), null = no expiry
     */
    @SuppressWarnings("ArrayRecordComponent") // defensive copies, equals and hashCode are overridden
    record Success(byte[] responseBytes, String authorizationId, @Nullable Instant sessionExpiry) implements RoundResult {

        /**
         * Canonical constructor with validation and defensive copy.
         */
        public Success {
            Objects.requireNonNull(responseBytes);
            Objects.requireNonNull(authorizationId);
            responseBytes = responseBytes.clone();
        }

        @Override
        public byte[] responseBytes() {
            return responseBytes.clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof Success(byte[] thatResponseBytes, String thatAuthorizationId, Instant thatSessionExpiry)
                    && Objects.equals(sessionExpiry, thatSessionExpiry)
                    && Arrays.equals(responseBytes, thatResponseBytes)
                    && Objects.equals(authorizationId, thatAuthorizationId);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(authorizationId, sessionExpiry);
            result = 31 * result + Arrays.hashCode(responseBytes);
            return result;
        }

        @Override
        public String toString() {
            return "Success{authorizationId='" + authorizationId + "', sessionExpiry=" + sessionExpiry + '}';
        }
    }

    /**
     * Authentication failed.
     *
     * @param responseBytes the error response bytes to send to the client (may be empty)
     * @param exception the exception describing the authentication failure
     */
    @SuppressWarnings("ArrayRecordComponent") // defensive copies, equals and hashCode are overridden
    record Failure(byte[] responseBytes, Exception exception) implements RoundResult {

        /**
         * Canonical constructor with validation and defensive copy.
         */
        public Failure {
            Objects.requireNonNull(responseBytes);
            Objects.requireNonNull(exception);
            responseBytes = responseBytes.clone();
        }

        @Override
        public byte[] responseBytes() {
            return responseBytes.clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof Failure(byte[] thatResponseBytes, Exception thatException)
                    && Arrays.equals(responseBytes, thatResponseBytes)
                    && Objects.equals(exception, thatException);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(exception);
            result = 31 * result + Arrays.hashCode(responseBytes);
            return result;
        }

        @Override
        public String toString() {
            return "Failure{exception=" + exception.getMessage() + '}';
        }
    }
}
