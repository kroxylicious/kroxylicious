/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.time.Duration;
import java.util.Optional;

public record NettySettings(Optional<Integer> workerThreadCount,
                            @Deprecated(since = "0.20.0", forRemoval = true) Optional<Integer> shutdownQuietPeriodSeconds,
                            Optional<Duration> shutdownQuietPeriod,
                            Optional<Duration> shutdownTimeout,
                            Optional<Duration> authenticatedIdleTimeout,
                            Optional<Duration> unauthenticatedIdleTimeout) {

    public NettySettings {
        shutdownQuietPeriod.ifPresent(d -> {
            if (d.isNegative()) {
                throw new IllegalArgumentException("shutdownQuietPeriod must not be negative: " + d);
            }
        });
        shutdownTimeout.ifPresent(d -> {
            if (d.isNegative()) {
                throw new IllegalArgumentException("shutdownTimeout must not be negative: " + d);
            }
        });
        authenticatedIdleTimeout.ifPresent(d -> {
            if (d.isNegative()) {
                throw new IllegalArgumentException("authenticatedIdleTimeout must not be negative: " + d);
            }
        });
        unauthenticatedIdleTimeout.ifPresent(d -> {
            if (d.isNegative()) {
                throw new IllegalArgumentException("unauthenticatedIdleTimeout must not be negative: " + d);
            }
        });
    }
}
