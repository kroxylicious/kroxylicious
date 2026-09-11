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

/**
 * The parts of a <a href="https://learn.microsoft.com/en-us/rest/api/keyvault/keys/get-key/get-key#jsonwebkey">
 * JSON Web Key (JWK)</a>, as returned by Azure Key Vault, that are used by the KMS provider.
 *
 * @param keyId the key id, in Object Identifier form.
 * @param keyType the type of the key, e.g. {@code RSA}.
 * @param keyOperations the operations supported by the key, e.g. {@code wrapKey} and {@code unwrapKey}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record JsonWebKey(@JsonProperty(required = true, value = "kid") String keyId,
                         @JsonProperty(required = true, value = "kty") String keyType,
                         @JsonProperty(required = true, value = "key_ops") List<String> keyOperations) {
    /**
     * Validates the record components.
     * @throws NullPointerException if any component is null.
     */
    public JsonWebKey {
        Objects.requireNonNull(keyId, "kid cannot be null");
        Objects.requireNonNull(keyType, "kty cannot be null");
        Objects.requireNonNull(keyOperations, "key_ops cannot be null");
    }
}
