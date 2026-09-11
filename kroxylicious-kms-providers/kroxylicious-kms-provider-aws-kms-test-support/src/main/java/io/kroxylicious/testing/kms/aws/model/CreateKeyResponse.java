/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.aws.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kms.provider.aws.kms.model.KeyMetadata;

/**
 * The response from a request to create a KMS key.
 *
 * @param keyMetadata metadata of the created key.
 * @see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_CreateKey.html">AWS API CreateKey</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CreateKeyResponse(@JsonProperty("KeyMetadata") KeyMetadata keyMetadata) {
    /**
     * Creates a CreateKeyResponse.
     *
     * @param keyMetadata metadata of the created key.
     */
    public CreateKeyResponse {
        Objects.requireNonNull(keyMetadata);
    }
}
