/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

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
 */
public record AuthenticationResult(
                                   Outcome outcome,
                                   byte[] responseBytes,
                                   @Nullable String authorizationId,
                                   @Nullable String errorMessage) {
    @Override
    public String toString() {
        // intentionally omit the responseBytes
        return "AuthenticationResult{" +
                "authorizationId='" + authorizationId + '\'' +
                ", outcome=" + outcome +
                ", errorMessage='" + errorMessage + '\'' +
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
     * Canonical constructor with validation.
     */
    public AuthenticationResult {
        Objects.requireNonNull(outcome, "outcome must not be null");
        Objects.requireNonNull(responseBytes, "responseBytes must not be null");
        if (outcome == Outcome.SUCCESS && authorizationId == null) {
            throw new IllegalArgumentException("authorizationId required for SUCCESS outcome");
        }
        if (outcome == Outcome.FAILURE && errorMessage == null) {
            throw new IllegalArgumentException("errorMessage required for FAILURE outcome");
        }
        if (outcome == Outcome.CHALLENGE && authorizationId != null) {
            throw new IllegalArgumentException("authorizationId must be null for CHALLENGE outcome");
        }
    }

    /**
     * Create a challenge result.
     *
     * @param responseBytes the challenge bytes
     * @return the authentication result
     */
    public static AuthenticationResult challenge(byte[] responseBytes) {
        return new AuthenticationResult(Outcome.CHALLENGE, responseBytes, null, null);
    }

    /**
     * Create a success result.
     *
     * @param responseBytes the final response bytes
     * @param authorizationId the authenticated user's authorization ID
     * @return the authentication result
     */
    public static AuthenticationResult success(byte[] responseBytes, String authorizationId) {
        return new AuthenticationResult(Outcome.SUCCESS, responseBytes, authorizationId, null);
    }

    /**
     * Create a failure result.
     *
     * @param responseBytes the error response bytes (may be empty)
     * @param errorMessage the error message
     * @return the authentication result
     */
    public static AuthenticationResult failure(byte[] responseBytes, String errorMessage) {
        return new AuthenticationResult(Outcome.FAILURE, responseBytes, null, errorMessage);
    }
}
