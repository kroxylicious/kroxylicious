/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a Vault authentication response.
 *
 * @param auth the auth object containing token details
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record VaultAuthResponse(Auth auth) {

    /**
     * Creates a new VaultAuthResponse.
     */
    public VaultAuthResponse {
        Objects.requireNonNull(auth);
    }

    /**
     * Represents the authentication details.
     *
     * @param clientToken the client token
     * @param leaseDuration the lease duration in seconds
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Auth(@JsonProperty("client_token") String clientToken, @JsonProperty("lease_duration") int leaseDuration) {

        /**
         * Creates a new Auth record.
         */
        public Auth {
            Objects.requireNonNull(clientToken);
        }
    }
}
