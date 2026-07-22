/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.time.Duration;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public record NettySettings(Optional<Integer> workerThreadCount,
                            Optional<Duration> shutdownQuietPeriod,
                            Optional<Duration> shutdownTimeout,
                            Optional<Duration> authenticatedIdleTimeout,
                            Optional<Duration> unauthenticatedIdleTimeout) {

    @JsonCreator
    public static NettySettings fromJson(
                                         @JsonProperty("workerThreadCount") Optional<Integer> workerThreadCount,
                                         @JsonProperty("shutdownQuietPeriod") Optional<Duration> shutdownQuietPeriod,
                                         @JsonProperty("shutdownTimeout") Optional<Duration> shutdownTimeout,
                                         @JsonProperty("authenticatedIdleTimeout") Optional<Duration> authenticatedIdleTimeout,
                                         @JsonProperty("unauthenticatedIdleTimeout") Optional<Duration> unauthenticatedIdleTimeout) {
        return new NettySettings(workerThreadCount, shutdownQuietPeriod, shutdownTimeout, authenticatedIdleTimeout, unauthenticatedIdleTimeout);
    }

    public NettySettings {
        requireNonNegative(shutdownQuietPeriod, "shutdownQuietPeriod");
        requireNonNegative(shutdownTimeout, "shutdownTimeout");
        requireNonNegative(authenticatedIdleTimeout, "authenticatedIdleTimeout");
        requireNonNegative(unauthenticatedIdleTimeout, "unauthenticatedIdleTimeout");
    }

    private static void requireNonNegative(Optional<Duration> value, String fieldName) {
        if (value.filter(Duration::isNegative).isPresent()) {
            throw new IllegalArgumentException(fieldName + " must not be negative: " + value.get());
        }
    }
}
