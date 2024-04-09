/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.time.Duration;
import java.util.function.Function;

import static java.util.Objects.requireNonNullElse;

public record KmsCacheConfig(
                             Integer decryptedDekCacheSize,
                             Duration decryptedDekExpireAfterAccessDuration,
                             Integer resolvedAliasCacheSize,
                             Duration resolvedAliasExpireAfterWriteDuration,
                             Duration resolvedAliasRefreshAfterWriteDuration,
                             Duration notFoundAliasExpireAfterWriteDuration) {

    public KmsCacheConfig {
        decryptedDekCacheSize = requireNonNullElse(decryptedDekCacheSize, 1000);
        decryptedDekExpireAfterAccessDuration = requireNonNullElse(decryptedDekExpireAfterAccessDuration, Duration.ofHours(1));
        resolvedAliasCacheSize = requireNonNullElse(resolvedAliasCacheSize, 1000);
        resolvedAliasExpireAfterWriteDuration = requireNonNullElse(resolvedAliasExpireAfterWriteDuration, Duration.ofMinutes(10));
        resolvedAliasRefreshAfterWriteDuration = requireNonNullElse(resolvedAliasRefreshAfterWriteDuration, Duration.ofMinutes(8));
        notFoundAliasExpireAfterWriteDuration = requireNonNullElse(notFoundAliasExpireAfterWriteDuration, Duration.ofSeconds(30));
    }

    KmsCacheConfig(Integer decryptedDekCacheSize,
                   Long decryptedDekExpireAfterAccessSeconds,
                   Integer resolvedAliasCacheSize,
                   Long resolvedAliasExpireAfterWriteSeconds,
                   Long resolvedAliasRefreshAfterWriteSeconds,
                   Long notFoundAliasExpireAfterWriteSeconds) {
        this(mapNotNull(decryptedDekCacheSize, Function.identity()),
                (Duration) mapNotNull(decryptedDekExpireAfterAccessSeconds, Duration::ofSeconds),
                mapNotNull(resolvedAliasCacheSize, Function.identity()),
                mapNotNull(resolvedAliasExpireAfterWriteSeconds, Duration::ofSeconds),
                mapNotNull(resolvedAliasRefreshAfterWriteSeconds, Duration::ofSeconds),
                mapNotNull(notFoundAliasExpireAfterWriteSeconds, Duration::ofSeconds));
    }

    static <T, Y> Y mapNotNull(T t, Function<T, Y> function) {
        return t == null ? null : function.apply(t);
    }

    private static final KmsCacheConfig DEFAULT_CONFIG = new KmsCacheConfig(null, (Long) null, null, null, null, null);
}
