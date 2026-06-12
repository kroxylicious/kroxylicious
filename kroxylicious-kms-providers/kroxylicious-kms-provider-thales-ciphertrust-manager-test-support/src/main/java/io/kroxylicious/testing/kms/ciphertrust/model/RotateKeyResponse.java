/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response from rotating a key in CipherTrust Manager.
 * In the real CTM, rotation creates a NEW key with a NEW ID.
 *
 * @param id unique key identifier (the new key's ID after rotation)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RotateKeyResponse(
                                @JsonProperty("id") String id) {

    public RotateKeyResponse {
        Objects.requireNonNull(id);
    }
}
