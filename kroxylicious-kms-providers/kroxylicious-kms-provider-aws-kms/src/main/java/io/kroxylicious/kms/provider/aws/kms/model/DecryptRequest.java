/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A request to the AWS KMS <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_Decrypt.html">Decrypt</a> operation.
 *
 * @param keyId id of the key to use for the decryption.
 * @param ciphertextBlob ciphertext to decrypt.
 */
@SuppressWarnings("java:S6218") // we don't need DecryptRequest equality
public record DecryptRequest(@JsonProperty(value = "KeyId") String keyId,
                             @SuppressWarnings("ArrayRecordComponent") @JsonProperty(value = "CiphertextBlob") byte[] ciphertextBlob) { // byte[] retained: transient Jackson DTO, equality unused
    /**
     * Creates the decrypt request.
     *
     * @param keyId id of the key to use for the decryption.
     * @param ciphertextBlob ciphertext to decrypt.
     */
    public DecryptRequest {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(ciphertextBlob);
    }
}
