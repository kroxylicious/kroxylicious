/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.auth;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({ "token_type", "expires_in", "access_token" })
public record AccessTokenResponse(@JsonProperty(value = "token_type", required = true) String tokenType,
                                  @JsonProperty(value = "expires_in", required = true) int expiresIn,
                                  @JsonProperty(value = "access_token", required = true) String accessToken) {
    public AccessTokenResponse {
        Objects.requireNonNull(tokenType, "token_type is required");
        if (expiresIn <= 0) {
            throw new IllegalArgumentException("expires_in must be greater than 0");
        }
        Objects.requireNonNull(accessToken, "access_token is required");
    }

    @Override
    public String toString() {
        return "AccessTokenResponse{" +
                "tokenType='" + tokenType + '\'' +
                ", expiresIn=" + expiresIn +
                ", accessToken='***************'" +
                '}';
    }
}
