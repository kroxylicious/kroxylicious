/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.slf4j;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Override the level at which the given action will be logged
 * @param action The action.
 * @param logAt The level to log this action at (both successes and failures).
 * @param logSuccessAt The level to log this action at for successes (when the action's status is null).
 * @param logFailureAt The level to log this action at for failures (when the action's status is nonnull).
 */
public record LevelExceptionConfig(
                                   @JsonProperty(required = true) String action,
                                   @Nullable Level logAt,
                                   @Nullable Level logSuccessAt,
                                   @Nullable Level logFailureAt) {
    public LevelExceptionConfig {
        Objects.requireNonNull(action);
        if (logAt != null) {
            if (logSuccessAt != null || logFailureAt != null) {
                throw new IllegalStateException("'logAt' is mutually exclusive with 'logSuccessAt' or 'logFailureAt'.");
            }
        }
        else {
            if (logSuccessAt == null && logFailureAt == null) {
                throw new IllegalStateException("At least one of 'logAt', 'logSuccessAt' or 'logFailureAt' must be specified.");
            }
        }
    }
}
