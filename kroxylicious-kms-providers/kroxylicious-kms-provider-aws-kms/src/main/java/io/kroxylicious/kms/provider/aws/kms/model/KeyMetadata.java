/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metadata about an AWS KMS key, as returned by the
 * <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_DescribeKey.html">DescribeKey</a> operation.
 *
 * @param keyId globally unique identifier for the key.
 * @param arn Amazon Resource Name of the key.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record KeyMetadata(@JsonProperty("KeyId") String keyId,
                          @JsonProperty("Arn") String arn) {
    /**
     * Creates the key metadata.
     *
     * @param keyId globally unique identifier for the key.
     * @param arn Amazon Resource Name of the key.
     */
    public KeyMetadata {
        Objects.requireNonNull(keyId);
        Objects.requireNonNull(arn);
    }
}
