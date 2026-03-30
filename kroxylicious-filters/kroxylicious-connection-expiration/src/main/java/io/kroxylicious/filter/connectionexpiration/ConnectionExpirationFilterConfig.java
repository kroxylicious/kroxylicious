/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.connectionexpiration;

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the {@link ConnectionExpirationFilter}.
 *
 * @param maxAge the maximum age of a connection before it will be closed; must be positive
 * @param jitter optional jitter to apply to the max age, randomizing the effective deadline within
 *               {@code [maxAge - jitter, maxAge + jitter]} per connection; must be non-negative and not greater than {@code maxAge}
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ConnectionExpirationFilterConfig(
                                               @JsonProperty(required = true) Duration maxAge,
                                               @JsonProperty @Nullable Duration jitter) {

    public ConnectionExpirationFilterConfig {
        if (maxAge.isNegative() || maxAge.isZero()) {
            throw new IllegalArgumentException("maxAge must be positive");
        }
        if (jitter != null && jitter.isNegative()) {
            throw new IllegalArgumentException("jitter must not be negative");
        }
        if (jitter != null && jitter.compareTo(maxAge) > 0) {
            throw new IllegalArgumentException("jitter must not be greater than maxAge");
        }
    }
}
