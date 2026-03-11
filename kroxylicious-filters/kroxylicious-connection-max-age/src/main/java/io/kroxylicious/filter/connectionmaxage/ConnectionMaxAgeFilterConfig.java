/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.connectionmaxage;

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for the {@link ConnectionMaxAgeFilter}.
 *
 * @param maxAgeSeconds the maximum age of a connection in seconds before it will be closed
 * @param jitterSeconds optional jitter in seconds to apply to the max age, randomizing the effective deadline within
 *                      {@code [maxAge - jitter, maxAge + jitter]} per connection
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ConnectionMaxAgeFilterConfig(
                                           @JsonProperty(required = true) long maxAgeSeconds,
                                           @JsonProperty Long jitterSeconds) {

    public ConnectionMaxAgeFilterConfig {
        if (maxAgeSeconds <= 0) {
            throw new IllegalArgumentException("maxAgeSeconds must be positive");
        }
        if (jitterSeconds != null && jitterSeconds < 0) {
            throw new IllegalArgumentException("jitterSeconds must not be negative");
        }
        if (jitterSeconds != null && jitterSeconds > maxAgeSeconds) {
            throw new IllegalArgumentException("jitterSeconds must not be greater than maxAgeSeconds");
        }
    }

    /**
     * Returns the max age as a {@link Duration}.
     * @return duration representing the max age
     */
    public Duration maxAgeDuration() {
        return Duration.ofSeconds(maxAgeSeconds);
    }

    /**
     * Returns the jitter as a {@link Duration}, or {@link Duration#ZERO} if not configured.
     * @return duration representing the jitter
     */
    public Duration jitterDuration() {
        return jitterSeconds == null ? Duration.ZERO : Duration.ofSeconds(jitterSeconds);
    }
}
