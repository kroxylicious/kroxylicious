/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.auth;

import java.time.Instant;
import java.util.Objects;

/**
 * A bearer token used to authenticate requests to Azure services.
 *
 * @param token the token.
 * @param created the instant at which the token was acquired.
 * @param expires the instant at which the token expires.
 */
public record BearerToken(String token, Instant created, Instant expires) {
    /**
     * Validates the record components.
     * @throws NullPointerException if any component is null.
     */
    public BearerToken {
        Objects.requireNonNull(token, "token is required");
        Objects.requireNonNull(created, "created is required");
        Objects.requireNonNull(expires, "expires is required");
    }

    boolean isExpired(Instant now) {
        return now.isAfter(expires);
    }

    @Override
    public String toString() {
        return "BearerToken{" +
                "token='********'" +
                ", created=" + created +
                ", expires=" + expires +
                '}';
    }
}
