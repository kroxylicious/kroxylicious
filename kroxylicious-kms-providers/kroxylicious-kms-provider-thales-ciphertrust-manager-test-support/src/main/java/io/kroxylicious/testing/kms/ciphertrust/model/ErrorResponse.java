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
 * Error response from CipherTrust Manager mock server.
 *
 * @param error error message
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ErrorResponse(
                            @JsonProperty("error") String error) {

    public ErrorResponse {
        Objects.requireNonNull(error);
    }
}
