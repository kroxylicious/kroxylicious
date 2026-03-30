/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.time.Duration;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public record NettySettings(Optional<Integer> workerThreadCount,
                            Optional<Duration> shutdownQuietPeriod,
                            Optional<Duration> shutdownTimeout,
                            Optional<Duration> authenticatedIdleTimeout,
                            Optional<Duration> unauthenticatedIdleTimeout) {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettySettings.class);

    /**
     * Jackson factory that also accepts the deprecated {@code shutdownQuietPeriodSeconds}
     * integer field, mapping it to {@code shutdownQuietPeriod} with a warning.
     */
    @JsonCreator
    public static NettySettings fromJson(
                                         @JsonProperty("workerThreadCount") Optional<Integer> workerThreadCount,
                                         @JsonProperty("shutdownQuietPeriod") Optional<Duration> shutdownQuietPeriod,
                                         @JsonProperty("shutdownTimeout") Optional<Duration> shutdownTimeout,
                                         @JsonProperty("authenticatedIdleTimeout") Optional<Duration> authenticatedIdleTimeout,
                                         @JsonProperty("unauthenticatedIdleTimeout") Optional<Duration> unauthenticatedIdleTimeout,
                                         @Deprecated(since = "0.20.0", forRemoval = true) @JsonProperty("shutdownQuietPeriodSeconds") Optional<Integer> shutdownQuietPeriodSeconds) {
        var resolvedQuietPeriod = shutdownQuietPeriod.or(() -> shutdownQuietPeriodSeconds.map(seconds -> {
            LOGGER.atWarn().log("shutdownQuietPeriodSeconds is deprecated, use shutdownQuietPeriod (Go-style duration e.g. \"2s\") instead");
            return Duration.ofSeconds(seconds);
        }));
        return new NettySettings(workerThreadCount, resolvedQuietPeriod, shutdownTimeout, authenticatedIdleTimeout, unauthenticatedIdleTimeout);
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
