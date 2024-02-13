/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
record CreateTokenResponse(Auth auth) {

    CreateTokenResponse {
        Objects.requireNonNull(auth);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Auth(@JsonProperty("client_token") String clientToken) {
        Auth {
            Objects.requireNonNull(clientToken);
        }
    }
}
