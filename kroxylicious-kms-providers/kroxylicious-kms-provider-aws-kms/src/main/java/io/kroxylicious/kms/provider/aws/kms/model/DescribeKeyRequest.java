/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A request to the AWS KMS <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_DescribeKey.html">DescribeKey</a> operation.
 *
 * @param keyId id or alias (prefixed {@code alias/}) of the key to describe.
 */
public record DescribeKeyRequest(@JsonProperty(value = "KeyId") String keyId) {

    /**
     * Creates the describe key request.
     *
     * @param keyId id or alias (prefixed {@code alias/}) of the key to describe.
     */
    public DescribeKeyRequest {
        Objects.requireNonNull(keyId);
    }
}
