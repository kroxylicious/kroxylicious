/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.NonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public record EncryptResponse(
        @JsonProperty(value = "status", required = true) int status,
        @JsonProperty(value = "body", required = true) @NonNull Response body) {

    public EncryptResponse {
        Objects.requireNonNull(body);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Response(
            @JsonProperty(value = "cipher", required = true) @NonNull byte[] cipher,
            @JsonProperty(value = "iv", required = true) @NonNull byte[] iv
    ) {

        public Response {
            Objects.requireNonNull(cipher);
            Objects.requireNonNull(iv);
        }
    }

}
