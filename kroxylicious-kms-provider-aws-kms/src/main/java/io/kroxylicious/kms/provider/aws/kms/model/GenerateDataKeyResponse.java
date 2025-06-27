/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("java:S6218") // we don't need DecryptResponse equality
public record GenerateDataKeyResponse(@JsonProperty(value = "KeyId") String keyId,
                                      @JsonProperty(value = "CiphertextBlob") byte[] ciphertextBlob,
                                      @JsonProperty(value = "Plaintext") byte[] plaintext) {

    public GenerateDataKeyResponse {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(ciphertextBlob);
        Objects.requireNonNull(plaintext);
    }
}
