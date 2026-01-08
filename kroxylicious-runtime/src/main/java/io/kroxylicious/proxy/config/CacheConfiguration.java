/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Cache Configuration
 * @param maxSize the maximum number of entries the cache may contain, null implies no maximum
 * @param expireAfterWriteSeconds cache entries should be automatically removed from the cache once this duration has elapsed after the entry's creation, or the most recent replacement of its value
 * @param expireAfterAccessSeconds cache entries should be automatically removed from the cache once this duration has elapsed after the entry's creation, creation, the most recent replacement of its value, or its last access
 */
public record CacheConfiguration(@Nullable Integer maxSize, @Nullable Long expireAfterWriteSeconds, @Nullable Long expireAfterAccessSeconds) {
    public static CacheConfiguration USE_CACHE_DEFAULTS = new CacheConfiguration(null, null, null);
}
