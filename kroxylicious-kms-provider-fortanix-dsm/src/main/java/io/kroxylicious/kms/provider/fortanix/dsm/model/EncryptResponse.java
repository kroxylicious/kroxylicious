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
 * Encrypt response from Fortanix DSM REST API, {@code /crypto/v1/encrypt}.
 *
 * @param kid The ID of the key used for encryption. Returned for non-transient keys.
 * @param cipher Encrypted ciphertext bytes.
 * @param iv The initialization vector used during encryption. This is only applicable for certain symmetric encryption modes.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("java:S6218") // we don't need EncryptResponse equality
public record EncryptResponse(
                              @JsonProperty(value = "kid", required = false) String kid,
                              @JsonProperty(value = "cipher", required = true) byte[] cipher,
                              @JsonProperty(value = "iv", required = true) byte[] iv) {

    public EncryptResponse {
        Objects.requireNonNull(cipher);
        Objects.requireNonNull(iv);
    }

    @Override
    public String toString() {
        return "EncryptResponse{" +
                "key='" + kid + '\'' +
                ", cipher='*********'" +
                ", iv='*********'" +
                '}';
    }
}
