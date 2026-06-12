/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

/**
 * Client credentials for Thales CipherTrust Manager authentication.
 * This is a placeholder for future implementation when client authentication support is added.
 *
 * @param clientId the client ID
 * @param clientSecret the client secret provider
 */
public record ClientCredentials(
                                @JsonProperty(required = true) String clientId,
                                @JsonProperty(required = true) PasswordProvider clientSecret) {
    /**
     * Constructs client credentials.
     */
    public ClientCredentials {
        Objects.requireNonNull(clientId, "clientId cannot be null");
        Objects.requireNonNull(clientSecret, "clientSecret cannot be null");
        if (clientId.isEmpty()) {
            throw new IllegalArgumentException("clientId cannot be empty");
        }
    }
}
