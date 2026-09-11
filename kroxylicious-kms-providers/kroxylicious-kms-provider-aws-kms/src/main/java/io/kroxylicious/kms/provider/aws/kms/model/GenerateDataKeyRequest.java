/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A request to the AWS KMS <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.html">GenerateDataKey</a> operation.
 *
 * @param keyId id of the key to use to generate the data key.
 * @param keySpec length of the data key, e.g. {@code AES_256}.
 */
public record GenerateDataKeyRequest(@JsonProperty(value = "KeyId") String keyId,
                                     @JsonProperty(value = "KeySpec") String keySpec) {

    /**
     * Creates the generate data key request.
     *
     * @param keyId id of the key to use to generate the data key.
     * @param keySpec length of the data key, e.g. {@code AES_256}.
     */
    public GenerateDataKeyRequest {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(keySpec);
    }
}
