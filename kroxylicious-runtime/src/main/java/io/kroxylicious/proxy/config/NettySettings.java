/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.time.Duration;
import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public record NettySettings(Optional<Integer> workerThreadCount, Optional<Integer> shutdownQuietPeriodSeconds,
                            @JsonSerialize(contentUsing = DurationSerde.Serializer.class) @JsonDeserialize(contentUsing = DurationSerde.Deserializer.class) Optional<Duration> unauthenticatedIdleTimeout,
                            @JsonSerialize(contentUsing = DurationSerde.Serializer.class) @JsonDeserialize(contentUsing = DurationSerde.Deserializer.class) Optional<Duration> authenticatedIdleTimeout) {

}
