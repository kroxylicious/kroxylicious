/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust;

import java.util.Objects;

/**
 * A reference to a CipherTrust wrapping key (KEK), including version information for cache invalidation.
 * The version changes on key rotation, allowing caching layers to detect key changes.
 *
 * @param name The stable key name (unchanged across rotations)
 * @param version The key version number (increments on rotation: 0, 1, 2, ...)
 */
public record WrappingKey(
                          String name,
                          long version) {

    /**
     * A CipherTrust wrapping key.
     */
    public WrappingKey {
        Objects.requireNonNull(name, "name must not be null");
    }
}
