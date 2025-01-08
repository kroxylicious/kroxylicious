/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Decrypt response from Fortanix DSM REST API, @code /crypto/v1/decrypt}.
 *
 * @param kid The ID of the key used for encryption. Returned for non-transient keys.
 * @param plain Decrypted plaintext bytes.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("java:S6218") // we don't need DecryptResponse equality
public record DecryptResponse(
                              @JsonProperty(value = "kid", required = false) String kid,
                              @JsonProperty(value = "plain", required = true) byte[] plain) {

    public DecryptResponse {
        Objects.requireNonNull(plain);
    }

    @Override
    public String toString() {
        return "DecryptResponse{" +
                "kid='" + kid + '\'' +
                ", plain='*********'" +
                '}';
    }
}
