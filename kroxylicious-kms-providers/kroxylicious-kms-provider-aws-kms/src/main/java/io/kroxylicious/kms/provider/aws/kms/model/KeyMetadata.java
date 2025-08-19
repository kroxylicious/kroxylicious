/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record KeyMetadata(@JsonProperty("KeyId") String keyId,
                          @JsonProperty("Arn") String arn) {
    public KeyMetadata {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(arn);
    }
}
