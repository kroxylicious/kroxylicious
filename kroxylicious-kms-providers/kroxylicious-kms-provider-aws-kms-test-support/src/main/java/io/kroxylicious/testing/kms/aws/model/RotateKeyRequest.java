/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.aws.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A request to perform an on-demand rotation of a KMS key.
 *
 * @param keyId id of the key to rotate.
 * @see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_RotateKeyOnDemand.html">AWS API RotateKeyOnDemand</a>
 */
public record RotateKeyRequest(@JsonProperty("KeyId") String keyId) {
    /**
     * Creates a RotateKeyRequest.
     *
     * @param keyId id of the key to rotate.
     */
    public RotateKeyRequest {
        Objects.requireNonNull(keyId);
    }
}
