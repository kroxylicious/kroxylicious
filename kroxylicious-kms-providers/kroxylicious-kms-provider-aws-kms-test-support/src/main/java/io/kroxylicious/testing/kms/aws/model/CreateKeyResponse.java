/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.aws.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kms.provider.aws.kms.model.KeyMetadata;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CreateKeyResponse(@JsonProperty("KeyMetadata") KeyMetadata keyMetadata) {
    public CreateKeyResponse {
        Objects.requireNonNull(keyMetadata);
    }
}
