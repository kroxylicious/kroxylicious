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

/**
 * Encrypt response from Fortanix DSM REST API.
 *
 * @param status status code
 * @param error error message
 * @param body encrypt response body
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public record EncryptResponse(@JsonProperty(value = "status", required = true) int status,
                              @JsonProperty(value = "error") String error,
                              @JsonProperty(value = "body") Response body)
        implements ResponseBodyContainer<EncryptResponse.Response> {

    /**
     * @param cipher Encrypted ciphertext bytes.
     * @param iv The initialization vector used during encryption. This is only applicable for certain symmetric encryption modes.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @SuppressWarnings("java:S6218") // we don't need EncryptResponse equality
    public record Response(
                           @JsonProperty(value = "cipher", required = true) @NonNull byte[] cipher,
                           @JsonProperty(value = "iv", required = true) @NonNull byte[] iv) {

        public Response {
            Objects.requireNonNull(cipher);
            Objects.requireNonNull(iv);
        }
    }

}
