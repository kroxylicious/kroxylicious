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
 * Response from creating a key in CipherTrust Manager.
 *
 * @param id unique key identifier
 * @param name key name
 * @param algorithm encryption algorithm
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CreateKeyResponse(
                                @JsonProperty("id") String id,
                                @JsonProperty("name") String name,
                                @JsonProperty("algorithm") String algorithm) {

    public CreateKeyResponse {
        Objects.requireNonNull(id);
        Objects.requireNonNull(name);
        Objects.requireNonNull(algorithm);
    }
}
