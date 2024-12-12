/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.session;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A session auth response.
 *
 * @param tokenType token type
 * @param expiresIn expiration value (in seconds)
 * @param accessToken access token
 * @param entityId entity id
 * @param allowedMfaMethods MFA methods
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SessionAuthResponse(@JsonProperty("token_type") String tokenType,
                                  @JsonProperty("expires_in") int expiresIn,
                                  @JsonProperty("access_token") String accessToken,
                                  @JsonProperty("entity_id") String entityId,
                                  @JsonProperty(value = "allowed_mfa_methods", required = false) List<String> allowedMfaMethods) {
    public SessionAuthResponse {
        Objects.requireNonNull(tokenType);
        Objects.requireNonNull(accessToken);
        Objects.requireNonNull(entityId);
    }

    @Override
    public String toString() {
        return "SessionAuthResponse{" +
                "tokenType='" + tokenType + '\'' +
                ", expiresIn=" + expiresIn +
                ", accessToken='*************'" +
                ", entityId='" + entityId + '\'' +
                ", allowedMfaMethods=" + allowedMfaMethods +
                '}';
    }
}
