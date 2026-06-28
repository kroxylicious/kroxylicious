/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public record GenerateDataKeyRequest(@JsonProperty(value = "KeyId") String keyId,
                                     @JsonProperty(value = "KeySpec") String keySpec) {

    public GenerateDataKeyRequest {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(keySpec);
    }
}
