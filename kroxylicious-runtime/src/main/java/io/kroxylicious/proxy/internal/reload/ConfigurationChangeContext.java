/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.Objects;

import io.kroxylicious.proxy.config.Configuration;

/**
 * Input passed to each {@link ChangeDetector} when an {@code applyConfiguration()} call is
 * orchestrated. Carries the currently-running configuration and the proposed new configuration
 * so that detectors can diff any pair of records they care about.
 *
 * @param oldConfig configuration currently applied to the running proxy
 * @param newConfig configuration the caller wants to apply
 */
public record ConfigurationChangeContext(Configuration oldConfig, Configuration newConfig) {

    public ConfigurationChangeContext {
        Objects.requireNonNull(oldConfig, "oldConfig");
        Objects.requireNonNull(newConfig, "newConfig");
    }
}
