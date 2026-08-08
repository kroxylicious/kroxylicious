/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.vault.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The response from a request to create a Vault token.
 *
 * @param auth authentication information, including the created token.
 * @see <a href="https://developer.hashicorp.com/vault/api-docs/auth/token#create-token">Vault API Create Token</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CreateTokenResponse(Auth auth) {

    /**
     * Creates a CreateTokenResponse.
     *
     * @param auth authentication information, including the created token.
     */
    public CreateTokenResponse {
        Objects.requireNonNull(auth);
    }

    /**
     * Authentication information associated with a created token.
     *
     * @param clientToken the client token.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Auth(@JsonProperty("client_token") String clientToken) {
        /**
         * Creates an Auth.
         *
         * @param clientToken the client token.
         */
        public Auth {
            Objects.requireNonNull(clientToken);
        }
    }
}
