/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record GetKeyResponse(@JsonProperty(required = true) JsonWebKey key, @JsonProperty(required = true) KeyAttributes attributes) {
    public GetKeyResponse {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(attributes, "attributes cannot be null");
    }
}
