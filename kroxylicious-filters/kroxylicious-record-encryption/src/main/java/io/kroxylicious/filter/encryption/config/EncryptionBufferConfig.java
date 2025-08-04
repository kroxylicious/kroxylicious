/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

/**
 *
 * @param minSizeBytes the minimum size of the encryption buffer
 * @param maxSizeBytes the maximum size of the encryption buffer
 */
public record EncryptionBufferConfig(int minSizeBytes, int maxSizeBytes) {
    public EncryptionBufferConfig {
        if (minSizeBytes <= 0) {
            throw new IllegalArgumentException("minSizeBytes must be greater than zero");
        }
        if (maxSizeBytes <= 0) {
            throw new IllegalArgumentException("maxSizeBytes must be greater than zero");
        }
        if (minSizeBytes > maxSizeBytes) {
            throw new IllegalArgumentException("minSizeBytes must be less than or equal to maxSizeBytes");
        }
    }
}
