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

/**
 * Tunable settings for a Netty event loop group.
 *
 * @param workerThreadCount number of worker threads; defaults to Netty's own default when absent
 * @param shutdownQuietPeriod quiet period observed during graceful shutdown of the event loop group
 * @param shutdownTimeout maximum time to wait for the event loop group to shut down
 * @param authenticatedIdleTimeout idle timeout applied to authenticated connections
 * @param unauthenticatedIdleTimeout idle timeout applied to connections that have not yet authenticated
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public record NettySettings(Optional<Integer> workerThreadCount,
                            Optional<Duration> shutdownQuietPeriod,
                            Optional<Duration> shutdownTimeout,
                            Optional<Duration> authenticatedIdleTimeout,
                            Optional<Duration> unauthenticatedIdleTimeout) {

    /**
     * Jackson creator.
     *
     * @param workerThreadCount number of worker threads
     * @param shutdownQuietPeriod quiet period observed during graceful shutdown
     * @param shutdownTimeout maximum time to wait for shutdown
     * @param authenticatedIdleTimeout idle timeout for authenticated connections
     * @param unauthenticatedIdleTimeout idle timeout for unauthenticated connections
     * @return the settings
     */
    @JsonCreator
    public static NettySettings fromJson(
                                         @JsonProperty("workerThreadCount") Optional<Integer> workerThreadCount,
                                         @JsonProperty("shutdownQuietPeriod") Optional<Duration> shutdownQuietPeriod,
                                         @JsonProperty("shutdownTimeout") Optional<Duration> shutdownTimeout,
                                         @JsonProperty("authenticatedIdleTimeout") Optional<Duration> authenticatedIdleTimeout,
                                         @JsonProperty("unauthenticatedIdleTimeout") Optional<Duration> unauthenticatedIdleTimeout) {
        return new NettySettings(workerThreadCount, shutdownQuietPeriod, shutdownTimeout, authenticatedIdleTimeout, unauthenticatedIdleTimeout);
    }

    /**
     * Validates that none of the configured durations are negative.
     */
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
