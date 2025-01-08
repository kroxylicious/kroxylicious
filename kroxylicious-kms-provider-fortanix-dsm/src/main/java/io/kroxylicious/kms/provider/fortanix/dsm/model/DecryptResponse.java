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
 * Decrypt response from Fortanix DSM REST API.
 *
 * @param status status code
 * @param error error message
 * @param body decrypt response body
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DecryptResponse(@JsonProperty(value = "status") int status,
                              @JsonProperty(value = "error") String error,
                              @JsonProperty(value = "body") Response body)
        implements ResponseBodyContainer<DecryptResponse.Response> {

    /**
     * @param plain Decrypted plaintext bytes.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @SuppressWarnings("java:S6218") // we don't need DecryptResponse equality
    public record Response(@JsonProperty(value = "plain") @NonNull byte[] plain) {
        public Response {
            Objects.requireNonNull(plain);
        }

    }
}
