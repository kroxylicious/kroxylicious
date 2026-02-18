/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Cache Configuration
 * @param maxSize the maximum number of entries the cache may contain, default (null) means no maximum
 * @param expireAfterWrite cache entries should be automatically removed from the cache once this duration has elapsed after the entry's creation, or the most recent replacement of its value. The default is to never expire.
 * @param expireAfterAccess cache entries should be automatically removed from the cache once this duration has elapsed after the entry's creation, creation, the most recent replacement of its value, or its last access. The default is 1 hour.
 */
public record CacheConfiguration(@Nullable Integer maxSize,
                                 @JsonSerialize(using = DurationSerde.Serializer.class) @JsonDeserialize(using = DurationSerde.Deserializer.class) @Nullable Duration expireAfterWrite,
                                 @JsonSerialize(using = DurationSerde.Serializer.class) @JsonDeserialize(using = DurationSerde.Deserializer.class) @Nullable Duration expireAfterAccess) {

    public static final CacheConfiguration DEFAULT = new CacheConfiguration(null, null, null);

    @Override
    public Duration expireAfterAccess() {
        return expireAfterAccess == null ? Duration.of(1L, ChronoUnit.HOURS) : expireAfterAccess;
    }
}
