/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response model for CipherTrust Manager authentication.
 *
 * @param jwt the JWT token
 * @param duration the token duration in seconds
 * @param refreshToken the refresh token for obtaining new JWTs
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AuthResponse(
                           @JsonProperty("jwt") String jwt,
                           @JsonProperty("duration") int duration,
                           @JsonProperty("refresh_token") String refreshToken) {}
