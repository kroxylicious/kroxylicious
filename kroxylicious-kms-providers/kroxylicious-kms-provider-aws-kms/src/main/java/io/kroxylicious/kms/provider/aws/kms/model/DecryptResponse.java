/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A response from the AWS KMS <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_Decrypt.html">Decrypt</a> operation.
 *
 * @param keyId id of the key used for the decryption.
 * @param plaintext decrypted plaintext.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("java:S6218") // we don't need DecryptResponse equality
public record DecryptResponse(@JsonProperty(value = "KeyId") String keyId,
                              @SuppressWarnings("ArrayRecordComponent") @JsonProperty(value = "Plaintext") byte[] plaintext) { // byte[] retained: transient Jackson DTO; plaintext key material must
                                                                                                                               // stay zeroable

    /**
     * Creates the decrypt response.
     *
     * @param keyId id of the key used for the decryption.
     * @param plaintext decrypted plaintext.
     */
    public DecryptResponse {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(plaintext);
    }
}
