/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Response model for CipherTrust Manager authentication.
 *
 * @param jwt the JWT token
 * @param duration the token duration in seconds
 * @param refreshToken the refresh token for obtaining new JWTs (null for client certificate authentication)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AuthResponse(
                           @JsonProperty("jwt") String jwt,
                           @JsonProperty("duration") int duration,
                           @JsonProperty("refresh_token") @Nullable String refreshToken) {

    /**
     * Constructs an authentication response.
     */
    public AuthResponse {
        Objects.requireNonNull(jwt, "jwt cannot be null");
        // refreshToken is nullable for client certificate authentication
    }
}
