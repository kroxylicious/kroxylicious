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

@JsonIgnoreProperties(ignoreUnknown = true)
public record WrapOrUnwrapResponse(@JsonProperty(value = "kid", required = true) String keyId, @JsonProperty(required = true) String value) {
    public WrapOrUnwrapResponse {
        Objects.requireNonNull(keyId, "keyId cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
    }

    public byte[] decodedValue() {
        return Base64.getUrlDecoder().decode(value);
    }
}
