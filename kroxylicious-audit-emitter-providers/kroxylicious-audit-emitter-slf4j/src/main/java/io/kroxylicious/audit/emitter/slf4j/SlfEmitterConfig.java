/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.slf4j;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the emitter.
 * @param logAt The level at which actions will be logged.
 * @param except Exceptions to the level specified by {@link #logAt() logAt}
 */
public record SlfEmitterConfig(
                               Level logAt,
                               List<LevelExceptionConfig> except) {
    @JsonCreator
    public SlfEmitterConfig(
                            @JsonProperty(required = true) Level logAt,
                            @Nullable List<LevelExceptionConfig> except) {
        this.logAt = Objects.requireNonNull(logAt, "logAt");
        this.except = Optional.ofNullable(except).orElse(List.of());
    }
}
