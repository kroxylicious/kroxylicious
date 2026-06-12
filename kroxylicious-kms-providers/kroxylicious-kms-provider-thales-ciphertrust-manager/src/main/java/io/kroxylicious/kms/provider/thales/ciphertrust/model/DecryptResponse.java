/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response model for CipherTrust Manager decryption operation.
 * Jackson automatically handles base64 decoding to byte[].
 *
 * @param plaintext the decrypted plaintext (base64 decoded by Jackson)
 */
@SuppressWarnings("java:S6218") // no need for toString, equals, hashCode to go deep on the byte[]
@JsonIgnoreProperties(ignoreUnknown = true)
public record DecryptResponse(
                              @SuppressWarnings("ArrayRecordComponent") @JsonProperty("plaintext") byte[] plaintext) {

    /**
     * Constructs a decrypt response.
     */
    public DecryptResponse {
        Objects.requireNonNull(plaintext, "plaintext cannot be null");
    }
}
