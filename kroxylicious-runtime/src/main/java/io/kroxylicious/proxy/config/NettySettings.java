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

}
