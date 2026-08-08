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
 * A response from the AWS KMS <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.html">GenerateDataKey</a> operation.
 *
 * @param keyId id of the key used to generate the data key.
 * @param ciphertextBlob data key encrypted by the key.
 * @param plaintext plaintext data key.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("java:S6218") // we don't need DecryptResponse equality
public record GenerateDataKeyResponse(@JsonProperty(value = "KeyId") String keyId,
                                      @SuppressWarnings("ArrayRecordComponent") @JsonProperty(value = "CiphertextBlob") byte[] ciphertextBlob, // byte[] retained: transient Jackson DTO, equality
                                                                                                                                               // unused
                                      @SuppressWarnings("ArrayRecordComponent") @JsonProperty(value = "Plaintext") byte[] plaintext) { // byte[] retained: transient Jackson DTO; plaintext key material
                                                                                                                                       // must stay zeroable

    /**
     * Creates the generate data key response.
     *
     * @param keyId id of the key used to generate the data key.
     * @param ciphertextBlob data key encrypted by the key.
     * @param plaintext plaintext data key.
     */
    public GenerateDataKeyResponse {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(ciphertextBlob);
        Objects.requireNonNull(plaintext);
    }
}
