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
 * A response from the AWS KMS <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_DescribeKey.html">DescribeKey</a> operation.
 *
 * @param keyMetadata metadata of the described key.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DescribeKeyResponse(@JsonProperty(value = "KeyMetadata") KeyMetadata keyMetadata) {

    /**
     * Creates the describe key response.
     *
     * @param keyMetadata metadata of the described key.
     */
    public DescribeKeyResponse {
        Objects.requireNonNull(keyMetadata);
    }
}
