/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.NonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DescribeKeyResponse(
        @JsonProperty(value = "KeyMetadata") @NonNull
        KeyMetadata keyMetadata
) {

    public DescribeKeyResponse {
        Objects.requireNonNull(keyMetadata);
    }
}
