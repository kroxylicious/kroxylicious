/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.Objects;

/**
 * Decrypt response.
 *
 * @param status status code
 * @param error error message
 * @param body decrypt response body
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("java:S6218") // we don't need DecryptResponse equality
public record DecryptResponse(@JsonProperty(value = "status") int status,
                              @JsonProperty(value = "error") String error,
                              @JsonProperty(value = "body") Response body) implements ResponseBodyContainer<DecryptResponse.Response> {


    /**
     * @param plain Decrypted plaintext bytes.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Response(@JsonProperty(value = "plain") @NonNull byte[] plain) {
        public Response {
            Objects.requireNonNull(plain);
        }

    }
}
