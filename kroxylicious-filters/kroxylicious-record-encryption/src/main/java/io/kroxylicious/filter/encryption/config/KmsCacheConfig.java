/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.time.Duration;
import java.util.function.Function;

import static java.util.Objects.requireNonNullElse;

/**
 * Configuration of the caches which sit in front of the KMS.
 *
 * @param decryptedDekCacheSize The maximum number of decrypted DEKs to cache.
 * @param decryptedDekExpireAfterAccessDuration How long after last access a decrypted DEK is removed from the cache.
 * @param resolvedAliasCacheSize The maximum number of resolved aliases to cache.
 * @param resolvedAliasExpireAfterWriteDuration How long after being cached a resolved alias is removed from the cache.
 * @param resolvedAliasRefreshAfterWriteDuration How long after being cached a resolved alias becomes eligible for a refresh.
 * @param notFoundAliasExpireAfterWriteDuration How long an unresolvable alias is remembered before the KMS is asked to resolve it again.
 * @param encryptionDekCacheRefreshAfterWriteDuration How long after being cached a DEK used for encryption becomes eligible for a refresh.
 * @param encryptionDekCacheExpireAfterWriteDuration How long after being cached a DEK used for encryption is removed from the cache.
 */
public record KmsCacheConfig(
                             Integer decryptedDekCacheSize,
                             Duration decryptedDekExpireAfterAccessDuration,
                             Integer resolvedAliasCacheSize,
                             Duration resolvedAliasExpireAfterWriteDuration,
                             Duration resolvedAliasRefreshAfterWriteDuration,
                             Duration notFoundAliasExpireAfterWriteDuration,
                             Duration encryptionDekCacheRefreshAfterWriteDuration,
                             Duration encryptionDekCacheExpireAfterWriteDuration) {

    /**
     * Creates a KMS cache config, applying defaults for any null argument.
     */
    public KmsCacheConfig {
        decryptedDekCacheSize = requireNonNullElse(decryptedDekCacheSize, 1000);
        decryptedDekExpireAfterAccessDuration = requireNonNullElse(decryptedDekExpireAfterAccessDuration, Duration.ofHours(1));
        resolvedAliasCacheSize = requireNonNullElse(resolvedAliasCacheSize, 1000);
        resolvedAliasExpireAfterWriteDuration = requireNonNullElse(resolvedAliasExpireAfterWriteDuration, Duration.ofMinutes(10));
        resolvedAliasRefreshAfterWriteDuration = requireNonNullElse(resolvedAliasRefreshAfterWriteDuration, Duration.ofMinutes(8));
        notFoundAliasExpireAfterWriteDuration = requireNonNullElse(notFoundAliasExpireAfterWriteDuration, Duration.ofSeconds(30));
        encryptionDekCacheRefreshAfterWriteDuration = requireNonNullElse(encryptionDekCacheRefreshAfterWriteDuration, Duration.ofHours(1));
        encryptionDekCacheExpireAfterWriteDuration = requireNonNullElse(encryptionDekCacheExpireAfterWriteDuration, Duration.ofHours(2));
    }

    @SuppressWarnings("java:S1905") // Sonar's warning about this is incorrect, the cast is required.
    KmsCacheConfig(Integer decryptedDekCacheSize,
                   Long decryptedDekExpireAfterAccessSeconds,
                   Integer resolvedAliasCacheSize,
                   Long resolvedAliasExpireAfterWriteSeconds,
                   Long resolvedAliasRefreshAfterWriteSeconds,
                   Long notFoundAliasExpireAfterWriteSeconds,
                   Long decryptedDekRefreshAfterWriteSeconds,
                   Long decryptedDekExpireAfterWriteSeconds) {
        this(mapNotNull(decryptedDekCacheSize, Function.identity()),
                (Duration) mapNotNull(decryptedDekExpireAfterAccessSeconds, Duration::ofSeconds),
                mapNotNull(resolvedAliasCacheSize, Function.identity()),
                mapNotNull(resolvedAliasExpireAfterWriteSeconds, Duration::ofSeconds),
                mapNotNull(resolvedAliasRefreshAfterWriteSeconds, Duration::ofSeconds),
                mapNotNull(notFoundAliasExpireAfterWriteSeconds, Duration::ofSeconds),
                mapNotNull(decryptedDekRefreshAfterWriteSeconds, Duration::ofSeconds),
                mapNotNull(decryptedDekExpireAfterWriteSeconds, Duration::ofSeconds));
    }

    static <T, Y> Y mapNotNull(T t, Function<T, Y> function) {
        return t == null ? null : function.apply(t);
    }

}
