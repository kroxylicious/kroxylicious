/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import static java.util.Objects.requireNonNullElse;

/**
 * Configuration for the {@link io.kroxylicious.filter.encryption.dek.DekManager}.
 *
 * @param maxEncryptionsPerDek maximum number of encryptions that will be performed by a DEK
 */
public record DekManagerConfig(Long maxEncryptionsPerDek) {
    public DekManagerConfig {
        maxEncryptionsPerDek = requireNonNullElse(maxEncryptionsPerDek, 5_000_000L);
    }
}
