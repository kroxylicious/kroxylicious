/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record JsonWebKey(@JsonProperty(required = true, value = "kid") String keyId,
                         @JsonProperty(required = true, value = "kty") String keyType,
                         @JsonProperty(required = true, value = "key_ops") List<String> keyOperations) {
    public JsonWebKey {
        Objects.requireNonNull(keyId, "kid cannot be null");
        Objects.requireNonNull(keyType, "kty cannot be null");
        Objects.requireNonNull(keyOperations, "key_ops cannot be null");
    }
}
