/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.util.Arrays;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Result of a SASL authentication exchange.
 * <p>
 * Represents the outcome of processing a SASL authenticate request, including
 * the response bytes to send back to the client and the final outcome.
 * </p>
 *
 * @param outcome the authentication outcome
 * @param responseBytes the bytes to send in the SASL authenticate response
 * @param authorizationId the authorization ID (only present on SUCCESS)
 * @param errorMessage the error message (only present on FAILURE)
 * @param sessionLifetimeMs session lifetime in milliseconds for reauthentication (KIP-368), 0 = no expiry
 */
@SuppressWarnings("ArrayRecordComponent") // defensive copies, equals and hashCode are overridden
public record AuthenticationResult(
                                   Outcome outcome,
                                   byte[] responseBytes,
                                   @Nullable String authorizationId,
                                   @Nullable String errorMessage,
                                   long sessionLifetimeMs) {

    /**
     * Canonical constructor with validation and defensive copy.
     */
    public AuthenticationResult {
        Objects.requireNonNull(outcome, "outcome must not be null");
        Objects.requireNonNull(responseBytes, "responseBytes must not be null");
        responseBytes = responseBytes.clone();
        if (outcome == Outcome.SUCCESS && authorizationId == null) {
            throw new IllegalArgumentException("authorizationId required for SUCCESS outcome");
        }
        if (outcome == Outcome.FAILURE && errorMessage == null) {
            throw new IllegalArgumentException("errorMessage required for FAILURE outcome");
        }
        if (outcome == Outcome.CHALLENGE && authorizationId != null) {
            throw new IllegalArgumentException("authorizationId must be null for CHALLENGE outcome");
        }
        if (sessionLifetimeMs < 0) {
            throw new IllegalArgumentException("sessionLifetimeMs must not be negative");
        }
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
        if (!(o instanceof AuthenticationResult that)) {
            return false;
        }
        return sessionLifetimeMs == that.sessionLifetimeMs
                && outcome == that.outcome
                && Arrays.equals(responseBytes, that.responseBytes)
                && Objects.equals(authorizationId, that.authorizationId)
                && Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(outcome, authorizationId, errorMessage, sessionLifetimeMs);
        result = 31 * result + Arrays.hashCode(responseBytes);
        return result;
    }

    @Override
    public String toString() {
        return "AuthenticationResult{" +
                "outcome=" + outcome +
                ", authorizationId='" + authorizationId + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                ", sessionLifetimeMs=" + sessionLifetimeMs +
                '}';
    }

    /**
     * Authentication outcome.
     */
    public enum Outcome {
        /**
         * Authentication requires another round (SCRAM).
         * The client must send another authenticate request.
         */
        CHALLENGE,

        /**
         * Authentication succeeded.
         * The {@link #authorizationId} field contains the authenticated user.
         */
        SUCCESS,

        /**
         * Authentication failed.
         * The {@link #errorMessage} field contains the failure reason.
         */
        FAILURE
    }

    /**
     * Create a challenge result.
     *
     * @param responseBytes the challenge bytes
     * @return the authentication result
     */
    public static AuthenticationResult challenge(byte[] responseBytes) {
        return new AuthenticationResult(Outcome.CHALLENGE, responseBytes, null, null, 0);
    }

    /**
     * Create a success result with no session lifetime (no reauthentication).
     *
     * @param responseBytes the final response bytes
     * @param authorizationId the authenticated user's authorization ID
     * @return the authentication result
     */
    public static AuthenticationResult success(byte[] responseBytes, String authorizationId) {
        return new AuthenticationResult(Outcome.SUCCESS, responseBytes, authorizationId, null, 0);
    }

    /**
     * Create a success result with a session lifetime for reauthentication (KIP-368).
     *
     * @param responseBytes the final response bytes
     * @param authorizationId the authenticated user's authorization ID
     * @param sessionLifetimeMs session lifetime in milliseconds (0 = no expiry)
     * @return the authentication result
     */
    public static AuthenticationResult success(byte[] responseBytes, String authorizationId, long sessionLifetimeMs) {
        return new AuthenticationResult(Outcome.SUCCESS, responseBytes, authorizationId, null, sessionLifetimeMs);
    }

    /**
     * Create a failure result.
     *
     * @param responseBytes the error response bytes (may be empty)
     * @param errorMessage the error message
     * @return the authentication result
     */
    public static AuthenticationResult failure(byte[] responseBytes, String errorMessage) {
        return new AuthenticationResult(Outcome.FAILURE, responseBytes, null, errorMessage, 0);
    }
}
