/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Client certificate authentication credentials for CipherTrust Manager.
 * <p>
 * The client certificate must be configured in the TLS configuration ({@code tls.key}).
 * The client ID is obtained during client registration with the CipherTrust Manager.
 * </p>
 *
 * @param clientId client ID obtained during client registration
 */
public record ClientCredentials(
                                @JsonProperty(value = "clientId", required = true) String clientId) {

    /**
     * Constructs client certificate authentication credentials.
     */
    public ClientCredentials {
        Objects.requireNonNull(clientId, "clientId cannot be null");
        if (clientId.isBlank()) {
            throw new IllegalArgumentException("clientId cannot be blank");
        }
    }

    @Override
    public String toString() {
        return "ClientCredentials{clientId='***'}";
    }
}
