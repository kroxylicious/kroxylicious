/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CreateTokenResponse(Auth auth) {

    public CreateTokenResponse {
        Objects.requireNonNull(auth);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Auth(@JsonProperty("client_token") String clientToken) {
        public Auth {
            Objects.requireNonNull(clientToken);
        }
    }
}
