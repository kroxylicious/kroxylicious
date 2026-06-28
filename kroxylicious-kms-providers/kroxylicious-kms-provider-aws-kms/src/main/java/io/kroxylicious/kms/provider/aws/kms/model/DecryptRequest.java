/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("java:S6218") // we don't need DecryptRequest equality
public record DecryptRequest(@JsonProperty(value = "KeyId") String keyId,
                             @JsonProperty(value = "CiphertextBlob") byte[] ciphertextBlob) {
    public DecryptRequest {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(ciphertextBlob);
    }
}
