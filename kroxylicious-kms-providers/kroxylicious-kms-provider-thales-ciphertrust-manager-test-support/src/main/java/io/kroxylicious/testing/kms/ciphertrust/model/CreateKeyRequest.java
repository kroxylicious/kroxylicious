/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust.model;

import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request for creating a key in CipherTrust Manager.
 *
 * @param name key name
 * @param algorithm encryption algorithm (e.g., "aes")
 * @param usageMask bitmask for key usage (e.g., 12 for encrypt + decrypt)
 * @param labels key-value labels for key metadata
 */
public record CreateKeyRequest(
                               @JsonProperty("name") String name,
                               @JsonProperty("algorithm") String algorithm,
                               @JsonProperty("usageMask") int usageMask,
                               @JsonProperty("labels") Map<String, String> labels) {

    /**
     * Constructs a create key request.
     */
    public CreateKeyRequest {
        Objects.requireNonNull(name);
        Objects.requireNonNull(algorithm);
        Objects.requireNonNull(labels);
    }
}
