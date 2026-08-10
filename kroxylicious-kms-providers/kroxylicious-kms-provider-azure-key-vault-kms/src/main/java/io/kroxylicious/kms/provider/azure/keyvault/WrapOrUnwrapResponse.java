/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.util.Base64;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The successful response to an Azure Key Vault wrap or unwrap operation.
 *
 * @param keyId the id of the key used for the operation, in Object Identifier form.
 * @param value the base64 url-encoded result of the operation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WrapOrUnwrapResponse(@JsonProperty(value = "kid", required = true) String keyId, @JsonProperty(required = true) String value) {
    /**
     * Validates the record components.
     * @throws NullPointerException if any component is null.
     */
    public WrapOrUnwrapResponse {
        Objects.requireNonNull(keyId, "keyId cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
    }

    /**
     * The result of the operation, base64 url-decoded.
     *
     * @return the decoded bytes.
     */
    public byte[] decodedValue() {
        return Base64.getUrlDecoder().decode(value);
    }
}
