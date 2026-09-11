/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The successful response to an Azure Key Vault
 * <a href="https://learn.microsoft.com/en-us/rest/api/keyvault/keys/get-key/get-key">get key</a> operation.
 *
 * @param key the public part of the key.
 * @param attributes the attributes of the key.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GetKeyResponse(@JsonProperty(required = true) JsonWebKey key, @JsonProperty(required = true) KeyAttributes attributes) {
    /**
     * Validates the record components.
     * @throws NullPointerException if any component is null.
     */
    public GetKeyResponse {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(attributes, "attributes cannot be null");
    }
}
